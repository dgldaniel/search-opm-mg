require 'httparty'
require 'nokogiri'
require 'json'
require 'csv'
require 'parallel'
require 'uri'
require 'fileutils'

class PrefeituraOPMScanner
  include HTTParty

  base_uri 'https://www.googleapis.com/customsearch/v1'

  GOOGLE_API_KEY = ENV["GOOGLE_API_KEY"]
  SEARCH_ENGINE_ID = ENV["SEARCH_ENGINE_ID"]

  LIMITE_MUNICIPIOS = nil

  DOMINIOS_INVALIDOS = [
    'instagram.com', 'facebook.com', 'twitter.com',
    'youtube.com', 'tiktok.com', 'wikipedia.org',
    'gov.br'  # domínio genérico federal, não municipal
  ].freeze

  # Confirmam existência de OPM com estrutura administrativa real
  OPM_ESTRUTURAL = [
    'secretaria da mulher',
    'secretaria municipal da mulher',
    'secretaria de políticas para mulheres',
    'secretaria politicas mulheres',
    'coordenadoria de políticas para mulheres',
    'coordenadoria politica das mulheres',
    'coordenadoria das mulheres',
    'coordenadoria da mulher',
    'diretoria de políticas para mulheres',
    'diretoria da mulher',
    'superintendência da mulher',
    'superintendencia da mulher',
    # 'procuradoria da mulher',
    'organismo de política para mulheres'
  ].freeze

  # Termos usados no contexto legislativo (câmara municipal / mg.leg.br)
  COORDENADORIA_VARIACOES_LEG = [
    'coordenadoria de políticas para mulheres',
    'coordenadoria politica das mulheres',
    'coordenadoria das mulheres',
    'coordenadoria da mulher',
    'procuradoria especial da mulher',
    'secretária da mulher',
  ].freeze

  OPM_FORMACAO_CONSELHO = [
    'conselho municipal dos direitos da mulher',
    'conselho municipal da mulher',
  ].freeze

  OPM_FORMACAO_FUNDO = [
    'fundo municipal da mulher',
  ].freeze

  OPM_FORMACAO_CONFERENCIA = [
    'conferência municipal de políticas para mulheres',
    'conferencia municipal de politicas para mulheres',
    'plano municipal de políticas para mulheres',
  ].freeze

  OPM_FORMACAO = (OPM_FORMACAO_CONSELHO + OPM_FORMACAO_FUNDO + OPM_FORMACAO_CONFERENCIA).freeze

  # Palavras genéricas — NÃO indicam OPM (ruído)
  OPM_RUIDO = [
    'violência contra mulher',
    'violencia contra mulher',
    'proteção à mulher',
    'protecao a mulher',
    'agosto lilás',
    'agosto lilas',
    'dia da mulher',
    'lei maria da penha',
    'casa da mulher',
    'centro de referência'
  ].freeze

  QUERIES_POR_DIA = 100
  DELAY_ENTRE_QUERIES = 2

  DATA_DIR = './dados'
  CHECKPOINT_FILE = "#{DATA_DIR}/checkpoint.json"
  CSV_OUTPUT = "#{DATA_DIR}/prefeituras_opm_resultado.csv"
  JSON_OUTPUT = "#{DATA_DIR}/prefeituras_opm_resultado.json"
  JSON_FORMATADO = "#{DATA_DIR}/prefeituras_opm_resultado_formatado.json"
  LOG_FILE = "#{DATA_DIR}/scanner.log"

  @@csv_mutex = Mutex.new
  @@json_mutex = Mutex.new

  def initialize(csv_municipios)
    criar_diretorios
    setup_logging

    @municipios = carregar_municipios(csv_municipios)
    @municipios = @municipios.first(LIMITE_MUNICIPIOS) if LIMITE_MUNICIPIOS

    @resultados = []
    @queries_realizadas = 0
    @checkpoint = carregar_checkpoint

    criar_csv_inicial
  end

  def executar_varredura
    puts "🔍 Scanner OPM em Sites de Prefeituras"
    log_info("Iniciando varredura")

    @municipios.each_with_index do |municipio, index|
      if @checkpoint && index < @checkpoint['ultima_posicao']
        puts "⏭️  Pulando #{municipio[:nome]} (já processado)"
        next
      end

      puts "▶️  [#{index+1}/#{@municipios.count}] Processando #{municipio[:nome]}..."
      log_info("Processando #{municipio[:nome]} (#{municipio[:codigo]})")

      sleep(DELAY_ENTRE_QUERIES)

      begin
        # Passo 1: Buscar site da prefeitura com validação de domínio (máx 2 queries)
        site_prefeitura = encontrar_site_prefeitura(municipio)

        urls_opm = []

        if site_prefeitura
          puts "   ✅ Site encontrado: #{site_prefeitura}"
          # Passo 2: Buscar OPM no site da prefeitura (1 query)
          urls_opm += buscar_opm_no_site(municipio, site_prefeitura)
        else
          puts "   ❌ Site não encontrado"
        end

        # Passo 3: Buscar na câmara municipal — mg.leg.br (1 query)
        urls_camara = buscar_opm_camara(municipio)
        urls_opm += urls_camara
        puts "   🏛️  #{urls_camara.count} resultado(s) na câmara" if urls_camara.any?

        # Passo 4: Buscar estrutura administrativa direta (1 query)
        urls_estrutura = buscar_estrutura_administrativa(municipio)
        urls_opm += urls_estrutura
        puts "   🔎 #{urls_estrutura.count} resultado(s) em busca estrutural direta" if urls_estrutura.any?

        # Passo 5: Classificar resultado (processamento local)
        classificacao = classificar_resultado(urls_opm)
        puts "   📊 Classificação: #{classificacao}"

        registrar_resultado(municipio, site_prefeitura, urls_opm, classificacao, index)
        salvar_checkpoint(index, municipio[:nome])
      rescue => e
        puts "   ❌ ERRO: #{e.message}"
        log_error("Erro em #{municipio[:nome]}: #{e.message}\n#{e.backtrace.join("\n")}")
        salvar_checkpoint(index, municipio[:nome])
      end
    end

    exibir_resumo
    limpar_checkpoint
  end

  private

  def criar_diretorios
    FileUtils.mkdir_p(DATA_DIR)
    [CHECKPOINT_FILE, CSV_OUTPUT, JSON_OUTPUT, JSON_FORMATADO, LOG_FILE].each do |f|
      FileUtils.touch(f) unless File.exist?(f)
    end
  end

  def setup_logging
    @logger = File.open(LOG_FILE, 'a')
    log_info("="*80)
    log_info("Scanner iniciado em #{Time.now}")
    log_info("="*80)
  end

  def log_info(msg); @logger.puts "[INFO] #{Time.now} - #{msg}"; @logger.flush; end
  def log_error(msg); @logger.puts "[ERROR] #{Time.now} - #{msg}"; @logger.flush; end

  def carregar_municipios(csv_file)
    CSV.read(csv_file, headers: true).map { |row| {nome: row['nome'], codigo: row['codigo_ibge']} }
  rescue => e
    log_error("Erro ao carregar CSV: #{e.message}")
    []
  end

  def carregar_checkpoint
    JSON.parse(File.read(CHECKPOINT_FILE)) if File.exist?(CHECKPOINT_FILE)
  rescue; nil; end

  def salvar_checkpoint(posicao, nome)
    checkpoint = {
      'ultima_posicao' => posicao+1,
      'ultimo_municipio' => nome,
      'timestamp' => Time.now.strftime('%Y-%m-%d %H:%M:%S'),
      'queries_realizadas' => @queries_realizadas
    }
    File.write(CHECKPOINT_FILE, JSON.pretty_generate(checkpoint))
  end

  def limpar_checkpoint
    File.delete(CHECKPOINT_FILE) if File.exist?(CHECKPOINT_FILE)
    log_info("Checkpoint removido")
  end

  def criar_csv_inicial
    return if File.size?(CSV_OUTPUT)
    CSV.open(CSV_OUTPUT, 'w') do |csv|
      csv << ['Posição','Município','Código IBGE','Site Prefeitura',
              'Classificação','Quantidade URLs OPM','URLs OPM (JSON)','Timestamp']
    end
  end

  def google_get(params, tentativa: 1)
    response = self.class.get('', query: params, timeout: 15)
    @queries_realizadas += 1

    if response.code == 429
      erro = response.parsed_response
      motivo = erro.dig('error', 'errors', 0, 'reason') rescue nil

      if motivo == 'dailyLimitExceeded' || motivo == 'rateLimitExceeded'
        puts "\n⛔ Limite diário da API atingido (#{@queries_realizadas} queries). Encerrando."
        log_error("Limite diário atingido após #{@queries_realizadas} queries.")
        exibir_resumo
        exit 1
      end

      if tentativa <= 4
        espera = 2 ** tentativa
        puts "   ⏳ 429 recebido — aguardando #{espera}s (tentativa #{tentativa}/4)..."
        log_error("429 recebido, retry #{tentativa} em #{espera}s")
        sleep(espera)
        return google_get(params, tentativa: tentativa + 1)
      else
        log_error("429 persistente após 4 tentativas")
        return nil
      end
    end

    response
  end

  def encontrar_site_prefeitura(municipio)
    # Tentativa 1: busca direta no domínio mg.gov.br
    query = "prefeitura \"#{municipio[:nome]}\" site:mg.gov.br"
    response = google_get({key: GOOGLE_API_KEY, cx: SEARCH_ENGINE_ID, q: query, num: 3})

    return nil unless response

    if response.code == 200 && response.parsed_response['items']
      response.parsed_response['items'].each do |item|
        url = item['link']
        dominio = URI.parse(url).host rescue nil
        next unless dominio
        next if DOMINIOS_INVALIDOS.any? { |d| dominio.include?(d) }
        return url
      end
    end

    # Tentativa 2: fallback com validação estrita de domínio
    query2 = "prefeitura de #{municipio[:nome]} mg site oficial"
    response2 = google_get({key: GOOGLE_API_KEY, cx: SEARCH_ENGINE_ID, q: query2, num: 5})
    return nil unless response2

    if response2.code == 200 && response2.parsed_response['items']
      response2.parsed_response['items'].each do |item|
        url = item['link']
        dominio = URI.parse(url).host rescue nil
        next unless dominio
        next if DOMINIOS_INVALIDOS.any? { |d| dominio.include?(d) }
        next unless dominio.end_with?('.mg.gov.br') || dominio.end_with?('.mg.leg.br')
        return url
      end
    end

    nil
  rescue => e
    log_error("Erro site #{municipio[:nome]}: #{e.message}")
    nil
  end

  def buscar_opm_no_site(municipio, site_url)
    dominio = extrair_dominio(site_url)
    todas_keywords = (OPM_ESTRUTURAL + COORDENADORIA_VARIACOES_LEG + OPM_FORMACAO).uniq
    query = "site:#{dominio} (#{todas_keywords.map{|k| "\"#{k}\""}.join(' OR ')})"
    response = google_get({key: GOOGLE_API_KEY, cx: SEARCH_ENGINE_ID, q: query, num: 10})
    return [] unless response

    urls_opm = []
    if response.code == 200
      if response.parsed_response['items']
        response.parsed_response['items'].each do |item|
          texto = "#{item['title']} #{item['snippet']}".downcase
          palavras = encontrar_palavras_chave(texto)
          next if palavras.empty?
          urls_opm << {
            url: item['link'],
            titulo: item['title'],
            snippet: item['snippet'][0..200],
            palavras_chave_encontradas: palavras,
            fonte: 'prefeitura'
          }
        end
        puts urls_opm.any? ? "   📍 #{urls_opm.count} URLs encontradas" : "   📍 Nenhuma URL encontrada"
      else
        log_error("Sem items para #{municipio[:nome]}: #{response.parsed_response.inspect}")
      end
    else
      log_error("Erro HTTP #{response.code} em #{municipio[:nome]}: #{response.parsed_response.inspect}")
    end
    urls_opm
  rescue => e
    log_error("Erro OPM #{municipio[:nome]}: #{e.message}")
    []
  end

  def buscar_opm_camara(municipio)
    termos = COORDENADORIA_VARIACOES_LEG.map { |k| "\"#{k}\"" }.join(' OR ')
    query = "\"#{municipio[:nome]}\" (#{termos}) site:mg.leg.br"
    response = google_get({key: GOOGLE_API_KEY, cx: SEARCH_ENGINE_ID, q: query, num: 5})
    return [] unless response&.code == 200 && response.parsed_response['items']

    response.parsed_response['items'].filter_map do |item|
      # Filtra ruído: URL deve pertencer ao domínio do município alvo,
      # evitando falsos positivos onde o nome aparece como rua/bairro em outra cidade.
      next unless url_pertence_ao_municipio?(item['link'], municipio[:nome])

      texto = "#{item['title']} #{item['snippet']}".downcase
      palavras = (OPM_ESTRUTURAL + COORDENADORIA_VARIACOES_LEG + OPM_FORMACAO)
        .select { |k| texto.include?(k.downcase) }.uniq
      next if palavras.empty?

      {
        url: item['link'],
        titulo: item['title'],
        snippet: item['snippet'][0..200],
        palavras_chave_encontradas: palavras,
        fonte: 'camara'
      }
    end
  rescue => e
    log_error("Erro câmara #{municipio[:nome]}: #{e.message}")
    []
  end

  def buscar_estrutura_administrativa(municipio)
    query = "\"#{municipio[:nome]}\" (\"secretaria da mulher\" OR \"coordenadoria da mulher\" OR \"diretoria da mulher\") site:mg.gov.br"
    response = google_get({key: GOOGLE_API_KEY, cx: SEARCH_ENGINE_ID, q: query, num: 5})
    return [] unless response&.code == 200 && response.parsed_response['items']

    response.parsed_response['items'].filter_map do |item|
      # Filtra ruído: URL deve pertencer ao domínio do município alvo.
      next unless url_pertence_ao_municipio?(item['link'], municipio[:nome])

      texto = "#{item['title']} #{item['snippet']}".downcase
      palavras = OPM_ESTRUTURAL.select { |k| texto.include?(k.downcase) }
      next if palavras.empty?

      {
        url: item['link'],
        titulo: item['title'],
        snippet: item['snippet'][0..200],
        palavras_chave_encontradas: palavras,
        fonte: 'estrutura_direta'
      }
    end
  rescue => e
    log_error("Erro estrutura #{municipio[:nome]}: #{e.message}")
    []
  end

  def classificar_resultado(urls_opm)
    return 'sem_opm' if urls_opm.empty?

    todas_palavras = urls_opm
      .flat_map { |u| u[:palavras_chave_encontradas] || [] }
      .map(&:downcase)
      .uniq

    tem_estrutural = (OPM_ESTRUTURAL + COORDENADORIA_VARIACOES_LEG).any? do |k|
      todas_palavras.any? { |p| p.include?(k.downcase) }
    end

    tem_formacao = OPM_FORMACAO.any? do |k|
      todas_palavras.any? { |p| p.include?(k.downcase) }
    end

    so_ruido = todas_palavras.all? do |p|
      OPM_RUIDO.any? { |r| p.include?(r.downcase) }
    end

    if tem_estrutural
      'opm_confirmada'
    elsif tem_formacao
      'em_formacao'
    elsif so_ruido
      'sem_opm'
    else
      'inconclusivo'
    end
  end

  def encontrar_palavras_chave(texto)
    (OPM_ESTRUTURAL + COORDENADORIA_VARIACOES_LEG + OPM_FORMACAO).uniq.select { |k| texto.include?(k.downcase) }
  end

  def extrair_dominio(url); URI.parse(url).host rescue url; end

  # Remove acentos e caracteres não-alfanuméricos para comparação de domínios.
  def normalizar_nome_municipio(nome)
    nome.downcase
        .tr('áàãâä', 'aaaaa')
        .tr('éèêë',  'eeee')
        .tr('íìîï',  'iiii')
        .tr('óòõôö', 'ooooo')
        .tr('úùûü',  'uuuu')
        .tr('ç',     'c')
        .gsub(/[^a-z0-9]/, '')
  end

  # Verifica se a URL pertence ao domínio do município alvo,
  # evitando falsos positivos onde o nome do município aparece
  # como nome de rua ou bairro em outra cidade.
  def url_pertence_ao_municipio?(url, municipio_nome)
    host = URI.parse(url).host.to_s.downcase rescue ''
    host.include?(normalizar_nome_municipio(municipio_nome))
  end

  def registrar_resultado(municipio, site_prefeitura, urls_opm, classificacao, posicao)
    resultado = {
      'municipio' => municipio[:nome],
      'codigo_ibge' => municipio[:codigo],
      'site_prefeitura' => site_prefeitura,
      'classificacao' => classificacao,
      'opm_encontrada' => ['opm_confirmada', 'em_formacao'].include?(classificacao),
      'quantidade_urls' => urls_opm.count,
      'urls' => urls_opm || [],
      'timestamp' => Time.now.strftime('%Y-%m-%d %H:%M:%S')
    }

    registrar_csv(municipio, site_prefeitura, urls_opm || [], classificacao)

    @resultados << resultado
    exportar_json_incremental
    exportar_json_formatado_incremental

    log_info("✅ #{municipio[:nome]} processado - Classificação: #{classificacao} - URLs: #{urls_opm&.count || 0}")
  end

  def registrar_csv(municipio, site_prefeitura, urls_opm, classificacao)
    urls_json = (urls_opm || []).any? ? urls_opm.to_json : '[]'

    @@csv_mutex.synchronize do
      linhas_existentes = []
      CSV.foreach(CSV_OUTPUT, headers: true) { |row| linhas_existentes << row.to_h }

      nova_posicao = linhas_existentes.count + 1

      CSV.open(CSV_OUTPUT, 'w') do |csv|
        csv << ['Posição','Município','Código IBGE','Site Prefeitura',
                'Classificação','Quantidade URLs OPM','URLs OPM (JSON)','Timestamp']

        linhas_existentes.each do |row|
          csv << [row['Posição'],row['Município'],row['Código IBGE'],
                  row['Site Prefeitura'],row['Classificação'],
                  row['Quantidade URLs OPM'],row['URLs OPM (JSON)'],row['Timestamp']]
        end

        csv << [nova_posicao, municipio[:nome], municipio[:codigo],
                site_prefeitura, classificacao, (urls_opm || []).count,
                urls_json, Time.now.strftime('%Y-%m-%d %H:%M:%S')]
      end
    end
  end

  def exportar_json_incremental
    @@json_mutex.synchronize do
      json_formatado = {
        'configuracao' => {
          'opm_estrutural' => OPM_ESTRUTURAL,
          'opm_formacao' => OPM_FORMACAO
        },
        'resumo' => {
          'total_municipios_processados' => @resultados.count,
          'opm_confirmada' => @resultados.count { |r| r['classificacao'] == 'opm_confirmada' },
          'em_formacao' => @resultados.count { |r| r['classificacao'] == 'em_formacao' },
          'inconclusivo' => @resultados.count { |r| r['classificacao'] == 'inconclusivo' },
          'sem_opm' => @resultados.count { |r| r['classificacao'] == 'sem_opm' },
          'total_urls_opm' => @resultados.sum { |r| r['quantidade_urls'] },
          'data_ultima_atualizacao' => Time.now.strftime('%Y-%m-%d %H:%M:%S'),
          'queries_realizadas' => @queries_realizadas
        },
        'dados' => @resultados
      }
      File.write(JSON_OUTPUT, JSON.pretty_generate(json_formatado))
    end
  end

  def exportar_json_formatado_incremental
    @@json_mutex.synchronize do
      opm_confirmada = @resultados.select { |r| r['classificacao'] == 'opm_confirmada' }
      em_formacao    = @resultados.select { |r| r['classificacao'] == 'em_formacao' }
      inconclusivo   = @resultados.select { |r| r['classificacao'] == 'inconclusivo' }
      sem_opm        = @resultados.select { |r| r['classificacao'] == 'sem_opm' }

      palavras_chave_frequencia = {}
      @resultados.each do |municipio|
        (municipio['urls'] || []).each do |url|
          (url[:palavras_chave_encontradas] || []).each do |palavra|
            palavras_chave_frequencia[palavra] ||= 0
            palavras_chave_frequencia[palavra] += 1
          end
        end
      end

      palavras_ordenadas = palavras_chave_frequencia.sort_by { |_, count| -count }
      total = @resultados.count

      json_formatado = {
        'metadata' => {
          'titulo' => 'Scanner OPM - Organismos de Políticas para Mulheres em Minas Gerais',
          'descricao' => 'Varredura automática de sites de prefeituras mineiras em busca de menções a políticas para mulheres',
          'data_ultima_atualizacao' => Time.now.strftime('%Y-%m-%d %H:%M:%S'),
          'estado' => 'Minas Gerais',
          'total_municipios_processados' => total,
          'modo_teste' => LIMITE_MUNICIPIOS ? true : false,
          'limite_municipios' => LIMITE_MUNICIPIOS
        },
        'configuracao' => {
          'keywords_estruturais' => { 'total' => OPM_ESTRUTURAL.count, 'lista' => OPM_ESTRUTURAL },
          'keywords_formacao'    => { 'total' => OPM_FORMACAO.count,   'lista' => OPM_FORMACAO }
        },
        'resumo_geral' => {
          'total_analisado' => total,
          'opm_confirmada' => {
            'total' => opm_confirmada.count,
            'percentual' => total > 0 ? ((opm_confirmada.count.to_f / total) * 100).round(2) : 0
          },
          'em_formacao' => {
            'total' => em_formacao.count,
            'percentual' => total > 0 ? ((em_formacao.count.to_f / total) * 100).round(2) : 0
          },
          'inconclusivo' => {
            'total' => inconclusivo.count,
            'percentual' => total > 0 ? ((inconclusivo.count.to_f / total) * 100).round(2) : 0
          },
          'sem_opm' => {
            'total' => sem_opm.count,
            'percentual' => total > 0 ? ((sem_opm.count.to_f / total) * 100).round(2) : 0
          },
          'total_urls_opm' => @resultados.sum { |r| r['quantidade_urls'] }
        },
        'palavras_chave_mais_frequentes' => palavras_ordenadas.map do |palavra, count|
          { 'palavra' => palavra, 'ocorrencias' => count,
            'percentual' => total > 0 ? ((count.to_f / total) * 100).round(2) : 0 }
        end,
        'municipios_opm_confirmada' => {
          'total' => opm_confirmada.count,
          'lista' => opm_confirmada.map { |m| formatar_municipio_com_opm(m) }.sort_by { |m| m['municipio'] }
        },
        'municipios_em_formacao' => {
          'total' => em_formacao.count,
          'lista' => em_formacao.map { |m| formatar_municipio_com_opm(m) }.sort_by { |m| m['municipio'] }
        },
        'municipios_inconclusivos' => {
          'total' => inconclusivo.count,
          'lista' => inconclusivo.map { |m| formatar_municipio_com_opm(m) }.sort_by { |m| m['municipio'] }
        },
        'municipios_sem_opm' => {
          'total' => sem_opm.count,
          'lista' => sem_opm.map do |municipio|
            {
              'municipio'      => municipio['municipio'],
              'codigo_ibge'    => municipio['codigo_ibge'],
              'site_prefeitura' => municipio['site_prefeitura'],
              'timestamp'      => municipio['timestamp']
            }
          end.sort_by { |m| m['municipio'] }
        }
      }

      File.write(JSON_FORMATADO, JSON.pretty_generate(json_formatado))
    end
  end

  def formatar_municipio_com_opm(municipio)
    {
      'municipio'       => municipio['municipio'],
      'codigo_ibge'     => municipio['codigo_ibge'],
      'classificacao'   => municipio['classificacao'],
      'site_prefeitura' => municipio['site_prefeitura'],
      'quantidade_urls' => municipio['quantidade_urls'],
      'urls' => (municipio['urls'] || []).map.with_index do |url, i|
        {
          'posicao'                  => i + 1,
          'url'                      => url[:url],
          'titulo'                   => url[:titulo],
          'snippet'                  => url[:snippet],
          'palavras_chave_encontradas' => url[:palavras_chave_encontradas] || [],
          'fonte'                    => url[:fonte]
        }
      end,
      'timestamp' => municipio['timestamp']
    }
  end

  def exibir_resumo
    opm_confirmada = @resultados.count { |r| r['classificacao'] == 'opm_confirmada' }
    em_formacao    = @resultados.count { |r| r['classificacao'] == 'em_formacao' }
    inconclusivo   = @resultados.count { |r| r['classificacao'] == 'inconclusivo' }
    sem_opm        = @resultados.count { |r| r['classificacao'] == 'sem_opm' }
    total_urls     = @resultados.sum { |r| r['quantidade_urls'] }

    mensagem = "\n" + "="*80 + "\n"
    mensagem += "📊 RESUMO FINAL DA VARREDURA\n"
    mensagem += "="*80 + "\n"
    mensagem += "Municípios processados: #{@resultados.count}/#{@municipios.count}\n"
    mensagem += "✅ OPM confirmada:  #{opm_confirmada}\n"
    mensagem += "🔄 Em formação:     #{em_formacao}\n"
    mensagem += "❓ Inconclusivo:    #{inconclusivo}\n"
    mensagem += "❌ Sem OPM:         #{sem_opm}\n"
    mensagem += "📍 Total de URLs com OPM encontradas: #{total_urls}\n"
    mensagem += "Queries realizadas: #{@queries_realizadas}/#{QUERIES_POR_DIA}\n"
    mensagem += "="*80 + "\n\n"
    mensagem += "📁 Resultados:\n"
    mensagem += "  ✅ #{CSV_OUTPUT}\n"
    mensagem += "  ✅ #{JSON_OUTPUT}\n"
    mensagem += "  ✅ #{JSON_FORMATADO}\n"

    puts mensagem
    log_info(mensagem)
    @logger.close
  end
end

# ===== EXECUÇÃO =====
if __FILE__ == $0
  begin
    scanner = PrefeituraOPMScanner.new('municipios_minas_gerais.csv')
    scanner.executar_varredura
  rescue => e
    puts "❌ ERRO FATAL: #{e.message}"
    puts e.backtrace.join("\n")
  end
end
