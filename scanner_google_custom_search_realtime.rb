# scanner_google_custom_search_realtime.rb

require 'httparty'
require 'json'
require 'csv'
require 'parallel'
require 'fileutils'

class OPMScannerGoogleAPIRealtime
  include HTTParty
  
  base_uri 'https://www.googleapis.com/customsearch/v1'
  
  # Suas credenciais
  GOOGLE_API_KEY = 'AIzaSyCns71NFE0dN9Ij8ogLqP7sJdZEsaGleyU'
  SEARCH_ENGINE_ID = '80b8acf80e0e84a18'
  
  # Limite gratuito: 100 queries/dia
  QUERIES_POR_DIA = 5
  DELAY_ENTRE_QUERIES = 0.9 # segundos
  
  # Arquivo CSV que será atualizado em tempo real
  CSV_OUTPUT = 'opm_google_api_resultado.csv'
  JSON_OUTPUT = 'opm_google_api_resultado.json'
  
  # Mutex para evitar conflitos ao escrever no CSV
  @@csv_mutex = Mutex.new
  @@json_mutex = Mutex.new
  
  def initialize(csv_municipios)
    @municipios = carregar_municipios(csv_municipios)
    @resultados = []
    @queries_realizadas = 0
    @limite_atingido = false
    
    # Criar CSV com headers desde o início
    criar_csv_inicial
  end
  
  def executar_varredura
    puts "🔍 Scanner OPM com Google Custom Search API (Tempo Real)"
    puts "📊 Municípios a processar: #{@municipios.count}"
    puts "📈 Limite gratuito: #{QUERIES_POR_DIA} queries/dia"
    puts "⏱️  Delay entre queries: #{DELAY_ENTRE_QUERIES}s\n\n"
    puts "📁 Arquivo CSV será atualizado em tempo real: #{CSV_OUTPUT}\n\n"
    
    tempo_inicio = Time.now
    
    # Processar municípios sequencialmente para respeitar rate limit
    @municipios.each_with_index do |municipio, index|
      break if @limite_atingido
      
      puts "▶️  [#{index + 1}/#{@municipios.count}] Processando #{municipio[:nome]}..."
      
      sleep(DELAY_ENTRE_QUERIES)
      buscar_opm(municipio)
      
      # Mostrar progresso a cada 10 queries
      if @queries_realizadas % 10 == 0 && @queries_realizadas > 0
        tempo_decorrido = Time.now - tempo_inicio
        velocidade = @queries_realizadas / tempo_decorrido
        tempo_restante = ((@municipios.count - @queries_realizadas) / velocidade / 60).round(1)
        
        puts "   📊 Progresso: #{@queries_realizadas}/#{QUERIES_POR_DIA} queries"
        puts "   ⏱️  Tempo estimado restante: ~#{tempo_restante} minutos\n\n"
      end
    end
    
    tempo_total = Time.now - tempo_inicio
    exibir_resumo(tempo_total)
  end
  
  private
  
  def carregar_municipios(csv_file)
    municipios = []
    CSV.foreach(csv_file, headers: true) do |row|
      municipios << {
        nome: row['nome'],
        codigo: row['codigo_ibge']
      }
    end
    municipios
  rescue => e
    puts "❌ Erro ao carregar CSV: #{e.message}"
    []
  end
  
  def criar_csv_inicial
    @@csv_mutex.synchronize do
      CSV.open(CSV_OUTPUT, 'w') do |csv|
        csv << [
          'Posição',
          'Município',
          'Código IBGE',
          'OPM Encontrada',
          'Tipo',
          'URL',
          'Snippet',
          'Timestamp'
        ]
      end
    end
    puts "✅ Arquivo CSV criado: #{CSV_OUTPUT}\n"
  end
  
  def buscar_opm(municipio)
    # Verificar limite
    if @queries_realizadas >= QUERIES_POR_DIA
      puts "\n   ⚠️  LIMITE GRATUITO ATINGIDO (100 queries/dia)"
      puts "   💡 Próximas queries custam $5 por 1.000 ou aguarde amanhã\n\n"
      @limite_atingido = true
      registrar_resultado_csv(
        municipio,
        false,
        'limite_atingido',
        nil,
        'Limite de 100 queries gratuitas atingido'
      )
      return
    end
    
    nome = municipio[:nome]
    codigo = municipio[:codigo]
    
    # Query otimizada
    query = "site:mg.gov.br \"#{nome}\" \"organismo de política para mulheres\" OR \"OPM\" OR \"política para mulheres\""
    
    begin
      response = self.class.get(
        '',
        query: {
          key: GOOGLE_API_KEY,
          cx: SEARCH_ENGINE_ID,
          q: query,
          num: 5
        },
        timeout: 15
      )
      
      @queries_realizadas += 1
      
      if response.code == 200
        processar_resultado(municipio, response.parsed_response)
      else
        puts "   ❌ HTTP #{response.code}"
        registrar_resultado_csv(municipio, false, "http_#{response.code}", nil, "Erro HTTP #{response.code}")
      end
      
    rescue Timeout::Error
      puts "   ⏱️  Timeout"
      registrar_resultado_csv(municipio, false, 'timeout', nil, 'Timeout na requisição')
    rescue => e
      puts "   ❌ Erro: #{e.message[0..50]}"
      registrar_resultado_csv(municipio, false, 'erro', nil, e.message[0..100])
    end
  end
  
  def processar_resultado(municipio, response_body)
    nome = municipio[:nome]
    
    if response_body['items'] && response_body['items'].any?
      resultado = response_body['items'].first
      tipo = identificar_tipo_opm(resultado['title'], resultado['snippet'])
      
      puts "   ✅ OPM ENCONTRADA"
      puts "      Tipo: #{tipo}"
      puts "      URL: #{resultado['link']}"
      
      registrar_resultado_csv(
        municipio,
        true,
        tipo,
        resultado['link'],
        resultado['snippet'][0..150]
      )
    else
      puts "   ❌ Sem resultado"
      registrar_resultado_csv(municipio, false, nil, nil, 'Nenhum resultado encontrado')
    end
  end
  
  def identificar_tipo_opm(title, snippet)
    texto_completo = "#{title} #{snippet}".downcase
    
    case texto_completo
    when /secretaria.*mulher/i
      'Secretaria'
    when /coordenadoria.*mulher/i
      'Coordenadoria'
    when /diretoria.*mulher/i
      'Diretoria'
    when /gerência.*mulher/i
      'Gerência'
    when /núcleo.*mulher/i
      'Núcleo'
    when /superintendência.*mulher/i
      'Superintendência'
    else
      'Menção encontrada'
    end
  end
  
  def registrar_resultado_csv(municipio, encontrada, tipo, url, snippet)
    # CORRIGIDO: Declarar nova_linha aqui (escopo correto)
    nova_linha = {
      'Posição' => nil,  # Será preenchido logo
      'Município' => municipio[:nome],
      'Código IBGE' => municipio[:codigo],
      'OPM Encontrada' => encontrada,
      'Tipo' => tipo,
      'URL' => url,
      'Snippet' => snippet,
      'Timestamp' => Time.now.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Sincronizar acesso ao arquivo para evitar corrupção
    @@csv_mutex.synchronize do
      # Ler dados atuais
      linhas_existentes = []
      CSV.foreach(CSV_OUTPUT, headers: true) do |row|
        # CORRIGIDO: Converter Row para Hash
        linhas_existentes << row.to_h
      end
      
      # Adicionar nova linha com posição correta
      nova_posicao = linhas_existentes.count + 1
      nova_linha['Posição'] = nova_posicao
      
      # Reescrever arquivo com todos os dados
      CSV.open(CSV_OUTPUT, 'w') do |csv|
        csv << [
          'Posição',
          'Município',
          'Código IBGE',
          'OPM Encontrada',
          'Tipo',
          'URL',
          'Snippet',
          'Timestamp'
        ]
        
        # CORRIGIDO: Iterar corretamente sobre linhas
        linhas_existentes.each do |row|
          csv << [
            row['Posição'],
            row['Município'],
            row['Código IBGE'],
            row['OPM Encontrada'],
            row['Tipo'],
            row['URL'],
            row['Snippet'],
            row['Timestamp']
          ]
        end
        
        # Adicionar nova linha
        csv << [
          nova_linha['Posição'],
          nova_linha['Município'],
          nova_linha['Código IBGE'],
          nova_linha['OPM Encontrada'],
          nova_linha['Tipo'],
          nova_linha['URL'],
          nova_linha['Snippet'],
          nova_linha['Timestamp']
        ]
      end
    end
    
    # Também guardar em memória
    @resultados << nova_linha
  end
  
  def exibir_resumo(tempo_total)
    encontradas = @resultados.count { |r| r['OPM Encontrada'] == true || r['OPM Encontrada'] == 'true' }
    ausentes = @resultados.count { |r| r['OPM Encontrada'] == false || r['OPM Encontrada'] == 'false' }
    
    puts "\n" + "="*80
    puts "📊 RESUMO FINAL DA VARREDURA"
    puts "="*80
    puts "Municípios processados: #{@resultados.count}/#{@municipios.count}"
    puts "✅ OPM Encontradas: #{encontradas} (#{(encontradas.to_f/@resultados.count*100).round(1)}%)"
    puts "❌ OPM Não Encontradas: #{ausentes} (#{(ausentes.to_f/@resultados.count*100).round(1)}%)"
    puts "\n📈 Estatísticas de API:"
    puts "Queries realizadas: #{@queries_realizadas}/#{QUERIES_POR_DIA}"
    puts "Queries restantes hoje: #{QUERIES_POR_DIA - @queries_realizadas}"
    
    velocidade_media = @resultados.count / (tempo_total / 60)
    puts "\n⏱️  Tempo total: #{tempo_total.round(1)}s (~#{velocidade_media.round(1)} municípios/min)"
    
    if @queries_realizadas >= QUERIES_POR_DIA
      municipios_faltando = @municipios.count - @queries_realizadas
      dias_faltando = (municipios_faltando / QUERIES_POR_DIA.to_f).ceil
      custo_aproximado = ((municipios_faltando) / 1000.0 * 5).round(2)
      puts "\n💡 Estimativa para completar:"
      puts "   Dias (gratuito): ~#{dias_faltando} dias"
      puts "   Custo (ilimitado): ~$#{custo_aproximado}"
    end
    
    puts "="*80
    
    puts "\n📁 Resultados em tempo real:"
    puts "  ✅ #{CSV_OUTPUT}"
    puts "  ✅ #{JSON_OUTPUT}"
    
    # Exportar também em JSON
    exportar_json
  end
  
  def exportar_json
    @@json_mutex.synchronize do
      File.write(JSON_OUTPUT, JSON.pretty_generate(@resultados))
    end
  end
end

# ===== EXECUÇÃO =====

if __FILE__ == $0
  scanner = OPMScannerGoogleAPIRealtime.new('municipios_minas_gerais.csv')
  scanner.executar_varredura
end