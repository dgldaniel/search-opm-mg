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
  
  GOOGLE_API_KEY = 'AIzaSyCns71NFE0dN9Ij8ogLqP7sJdZEsaGleyU'
  SEARCH_ENGINE_ID = '80b8acf80e0e84a18'
  
  LIMITE_MUNICIPIOS = nil
  
  OPM_KEYWORDS = [
    'secretaria mulheres','secretaria da mulher','secretaria de políticas para mulheres',
    'coordenadoria mulheres','diretoria mulheres','núcleo mulheres','conselho mulheres',
    'proteção à mulher','política para mulheres','secretária política das mulheres',
    'secretaria política das mulheres','coordenadoria política das mulheres',
    'coordenadoria politica das mulheres','diretoria política das mulheres',
    'diretoria politica das mulheres','núcleo política das mulheres',
    'nucleo política das mulheres','nucleo politica das mulheres',
    'violência contra mulher','violencia contra mulher','ação mulheres','acao mulheres',
    'conselho política mulheres','conselho politica mulheres','sociedade civil',
    'violência contra mulheres','violencia contra mulheres',
    'organismo de política para mulheres','OPM'
  ].freeze
  
  QUERIES_POR_DIA = 100
  DELAY_ENTRE_QUERIES = 0.9
  
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
        site_prefeitura = encontrar_site_prefeitura(municipio)
        
        if site_prefeitura
          puts "   ✅ Site encontrado: #{site_prefeitura}"
          urls_opm = buscar_opm_no_site(municipio, site_prefeitura)
          registrar_resultado(municipio, site_prefeitura, urls_opm || [], index)
        else
          puts "   ❌ Site não encontrado"
          registrar_resultado(municipio, nil, [], index)
        end
        
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
              'OPM Encontrada','Quantidade URLs OPM','URLs OPM (JSON)','Timestamp']
    end
  end
  
  def encontrar_site_prefeitura(municipio)
    query = "site:mg.gov.br \"#{municipio[:nome]}\" prefeitura OR \"#{municipio[:nome]}\" OR município"
    response = self.class.get('', query: {key: GOOGLE_API_KEY, cx: SEARCH_ENGINE_ID, q: query, num: 3}, timeout: 15)
    @queries_realizadas += 1
    
    if response.code == 200
      if response.parsed_response['items']
        return response.parsed_response['items'].first['link']
      else
        log_error("Sem items para #{municipio[:nome]}: #{response.parsed_response.inspect}")
      end
    else
      log_error("Erro HTTP #{response.code} em #{municipio[:nome]}: #{response.parsed_response.inspect}")
    end
    nil
  rescue => e
    log_error("Erro site #{municipio[:nome]}: #{e.message}")
    nil
  end
  
  def buscar_opm_no_site(municipio, site_url)
    dominio = extrair_dominio(site_url)
    query = "site:#{dominio} (#{OPM_KEYWORDS.map{|k| "\"#{k}\""}.join(' OR ')})"
    response = self.class.get('', query: {key: GOOGLE_API_KEY, cx: SEARCH_ENGINE_ID, q: query, num: 10}, timeout: 15)
    @queries_realizadas += 1
    
    urls_opm = []
    if response.code == 200
      if response.parsed_response['items']
        response.parsed_response['items'].each do |item|
          texto = "#{item['title']} #{item['snippet']}".downcase
          urls_opm << {
            url: item['link'],
            titulo: item['title'],
            snippet: item['snippet'][0..200],
            palavras_chave_encontradas: encontrar_palavras_chave(texto)
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
  
  def encontrar_palavras_chave(texto)
    OPM_KEYWORDS.select { |k| texto.include?(k.downcase) }
  end
  
  def extrair_dominio(url); URI.parse(url).host rescue url; end
  
  def registrar_resultado(municipio, site_prefeitura, urls_opm, posicao)
    resultado = {
      'municipio' => municipio[:nome],
      'codigo_ibge' => municipio[:codigo],
      'site_prefeitura' => site_prefeitura,
      'opm_encontrada' => urls_opm.any?,
      'quantidade_urls' => urls_opm.count,
            'urls' => urls_opm || [],   # ✅ garante que nunca seja nil
      'timestamp' => Time.now.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    registrar_csv(municipio, site_prefeitura, urls_opm || [])
    
    @resultados << resultado
    exportar_json_incremental
    exportar_json_formatado_incremental
    
    log_info("✅ #{municipio[:nome]} processado - URLs encontradas: #{urls_opm&.count || 0}")
  end
  
  def registrar_csv(municipio, site_prefeitura, urls_opm)
    urls_json = (urls_opm || []).any? ? urls_opm.to_json : '[]'
    
    @@csv_mutex.synchronize do
      linhas_existentes = []
      CSV.foreach(CSV_OUTPUT, headers: true) { |row| linhas_existentes << row.to_h }
      
      nova_posicao = linhas_existentes.count + 1
      
      CSV.open(CSV_OUTPUT, 'w') do |csv|
        csv << ['Posição','Município','Código IBGE','Site Prefeitura',
                'OPM Encontrada','Quantidade URLs OPM','URLs OPM (JSON)','Timestamp']
        
        linhas_existentes.each do |row|
          csv << [row['Posição'],row['Município'],row['Código IBGE'],
                  row['Site Prefeitura'],row['OPM Encontrada'],
                  row['Quantidade URLs OPM'],row['URLs OPM (JSON)'],row['Timestamp']]
        end
        
        csv << [nova_posicao, municipio[:nome], municipio[:codigo],
                site_prefeitura, (urls_opm || []).any?, (urls_opm || []).count,
                urls_json, Time.now.strftime('%Y-%m-%d %H:%M:%S')]
      end
    end
  end
  
  def exportar_json_incremental
    @@json_mutex.synchronize do
      json_formatado = {
        'configuracao' => { 'opm_keywords' => OPM_KEYWORDS },
        'resumo' => {
          'total_municipios_processados' => @resultados.count,
          'com_opm' => @resultados.count { |r| r['opm_encontrada'] },
          'sem_opm' => @resultados.count { |r| !r['opm_encontrada'] },
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
      com_opm = @resultados.select { |r| r['opm_encontrada'] }
      sem_opm = @resultados.reject { |r| r['opm_encontrada'] }
      
      palavras_chave_frequencia = {}
      com_opm.each do |municipio|
        (municipio['urls'] || []).each do |url|
          (url['palavras_chave_encontradas'] || []).each do |palavra|
            palavras_chave_frequencia[palavra] ||= 0
            palavras_chave_frequencia[palavra] += 1
          end
        end
      end
      
      palavras_ordenadas = palavras_chave_frequencia.sort_by { |_, count| -count }
      
      json_formatado = {
        'metadata' => {
          'titulo' => 'Scanner OPM - Organismos de Políticas para Mulheres em Minas Gerais',
          'descricao' => 'Varredura automática de sites de prefeituras mineiras em busca de menções a políticas para mulheres',
          'data_ultima_atualizacao' => Time.now.strftime('%Y-%m-%d %H:%M:%S'),
          'estado' => 'Minas Gerais',
          'total_municipios_processados' => @resultados.count,
          'modo_teste' => LIMITE_MUNICIPIOS ? true : false,
          'limite_municipios' => LIMITE_MUNICIPIOS
        },
        'configuracao' => {
          'palavras_chave_opm' => { 'total' => OPM_KEYWORDS.count, 'lista' => OPM_KEYWORDS }
        },
        'resumo_geral' => {
          'total_analisado' => @resultados.count,
          'com_opm' => {
            'total' => com_opm.count,
            'percentual' => com_opm.count > 0 ? ((com_opm.count.to_f / @resultados.count) * 100).round(2) : 0
          },
          'sem_opm' => {
            'total' => sem_opm.count,
            'percentual' => sem_opm.count > 0 ? ((sem_opm.count.to_f / @resultados.count) * 100).round(2) : 0
          },
          'total_urls_opm' => @resultados.sum { |r| r['quantidade_urls'] },
          'urls_por_municipio_media' => com_opm.count > 0 ? (@resultados.sum { |r| r['quantidade_urls'] }.to_f / com_opm.count).round(2) : 0
        },
        'palavras_chave_mais_frequentes' => palavras_ordenadas.map do |palavra, count|
          { 'palavra' => palavra, 'ocorrencias' => count,
            'percentual' => com_opm.count > 0 ? ((count.to_f / com_opm.count) * 100).round(2) : 0 }
        end,
        'municipios_com_opm' => {
          'total' => com_opm.count,
          'lista' => com_opm.map do |municipio|
            {
              'municipio' => municipio['municipio'],
              'codigo_ibge' => municipio['codigo_ibge'],
              'site_prefeitura' => municipio['site_prefeitura'],
              'quantidade_urls' => municipio['quantidade_urls'],
              'urls' => (municipio['urls'] || []).map do |url|
                {
                  'posicao' => (municipio['urls'] || []).index(url) + 1,
                  'url' => url['url'],
                  'titulo' => url['titulo'],
                  'snippet' => url['snippet'],
                  'palavras_chave_encontradas' => url['palavras_chave_encontradas'] || []
                }
              end,
              'timestamp' => municipio['timestamp']
            }
          end.sort_by { |m| m['municipio'] }
        },
        'municipios_sem_opm' => {
          'total' => sem_opm.count,
          'lista' => sem_opm.map do |municipio|
            {
              'municipio' => municipio['municipio'],
              'codigo_ibge' => municipio['codigo_ibge'],
              'site_prefeitura' => municipio['site_prefeitura'],
              'timestamp' => municipio['timestamp']
            }
          end.sort_by { |m| m['municipio'] }
        }
      }
      
      File.write(JSON_FORMATADO, JSON.pretty_generate(json_formatado))
    end
  end
  
  def exibir_resumo
    com_opm = @resultados.count { |r| r['opm_encontrada'] }
    sem_opm = @resultados.count { |r| !r['opm_encontrada'] }
    total_urls = @resultados.sum { |r| r['quantidade_urls'] }
    
    mensagem = "\n" + "="*80 + "\n"
    mensagem += "📊 RESUMO FINAL DA VARREDURA\n"
    mensagem += "="*80 + "\n"
    mensagem += "Municípios processados: #{@resultados.count}/#{@municipios.count}\n"
    mensagem += "✅ Prefeituras com OPM: #{com_opm}\n"
    mensagem += "❌ Prefeituras sem OPM: #{sem_opm}\n"
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
