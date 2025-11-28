# scanner_diario_oficial_opm.rb

require 'httparty'
require 'nokogiri'
require 'json'
require 'csv'
require 'uri'

class DiarioOficialOPMScanner
  include HTTParty
  
  base_uri 'https://www.googleapis.com/customsearch/v1'
  
  # Suas credenciais
  GOOGLE_API_KEY = 'AIzaSyCns71NFE0dN9Ij8ogLqP7sJdZEsaGleyU'
  SEARCH_ENGINE_ID = '80b8acf80e0e84a18'
  
  # ⭐ LIMITE PARA TESTES
  LIMITE_MUNICIPIOS = 5
  
  # Termos para buscar portarias de nomeação
  PORTARIA_KEYWORDS = [
    'portaria nomeação OPM',
    'portaria organismo política mulheres',
    'designação OPM',
    'nomeação coordenadoria mulheres',
    'nomeação secretaria mulheres',
    'designação coordenadora mulheres',
    'portaria criação OPM',
    'decreto OPM',
    'resolução OPM'
  ].freeze
  
  QUERIES_POR_DIA = 100
  DELAY_ENTRE_QUERIES = 1.2
  
  CSV_OUTPUT = 'diario_oficial_opm_resultado.csv'
  JSON_OUTPUT = 'diario_oficial_opm_resultado.json'
  JSON_FORMATADO = 'diario_oficial_opm_resultado_formatado.json'
  
  @@csv_mutex = Mutex.new
  @@json_mutex = Mutex.new
  
  def initialize(csv_municipios)
    @municipios = carregar_municipios(csv_municipios)
    
    if LIMITE_MUNICIPIOS
      @municipios = @municipios.first(LIMITE_MUNICIPIOS)
    end
    
    @resultados = []
    @queries_realizadas = 0
    
    criar_csv_inicial
  end
  
  def executar_varredura
    modo = LIMITE_MUNICIPIOS ? "🧪 MODO TESTE (#{LIMITE_MUNICIPIOS} primeiros)" : "🔍 MODO COMPLETO"
    
    puts "#{modo} - Scanner Diário Oficial - OPM"
    puts "📊 Municípios a processar: #{@municipios.count}"
    puts "📈 Limite gratuito: #{QUERIES_POR_DIA} queries/dia"
    puts "📋 Termos de busca: #{PORTARIA_KEYWORDS.count}\n\n"
    
    tempo_inicio = Time.now
    
    @municipios.each_with_index do |municipio, index|
      puts "▶️  [#{index + 1}/#{@municipios.count}] Processando #{municipio[:nome]}..."
      
      sleep(DELAY_ENTRE_QUERIES)
      
      # ETAPA 1: Encontrar Diário Oficial do município
      diario_oficial = encontrar_diario_oficial(municipio)
      
      if diario_oficial
        puts "   ✅ Diário Oficial encontrado: #{diario_oficial[:url]}"
        
        # ETAPA 2: Buscar portarias de OPM no Diário Oficial
        portarias = buscar_portarias_opm(municipio, diario_oficial)
        
        registrar_resultado(municipio, diario_oficial, portarias)
      else
        puts "   ❌ Diário Oficial não encontrado"
        registrar_resultado(municipio, nil, [])
      end
      
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
  #   municipios = []
  #   CSV.foreach(csv_file, headers: true) do |row|
  #     municipios << {
  #       nome: row['nome'],
  #       codigo: row['codigo_ibge']
  #     }
  #   end
  #   municipios
  # rescue => e
  #   puts "❌ Erro ao carregar CSV: #{e.message}"
  #   []
  [{ nome: 'Belo Horizonte', codigo: '3106200' },
   { nome: 'Uberlândia', codigo: '3170206' },
   { nome: 'Contagem', codigo: '3118601' },
   { nome: 'Juiz de Fora', codigo: '3136702' },
   { nome: 'Betim', codigo: '3106702' }]
  end
  
  def criar_csv_inicial
    @@csv_mutex.synchronize do
      CSV.open(CSV_OUTPUT, 'w') do |csv|
        csv << [
          'Posição',
          'Município',
          'Código IBGE',
          'Diário Oficial URL',
          'Portarias Encontradas',
          'Quantidade Portarias',
          'Data Publicação Mais Recente',
          'Portarias (JSON)',
          'Timestamp'
        ]
      end
    end
    puts "✅ Arquivo CSV criado: #{CSV_OUTPUT}\n"
  end
  
  def encontrar_diario_oficial(municipio)
    nome = municipio[:nome]
    
    # Variações de nomes para Diário Oficial
    nomes_diario = [
      "diário oficial #{nome}",
      "diario oficial #{nome}",
      "jornal oficial #{nome}",
      "#{nome} diário oficial",
      "imprensa oficial #{nome}"
    ]
    
    nomes_diario.each do |termo|
      query = "site:mg.gov.br \"#{termo}\" OR \"#{nome}\" \"diário oficial\" OR \"diario oficial\""
      
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
        
        if response.code == 200 && response.parsed_response['items']
          response.parsed_response['items'].each do |item|
            # Verificar se é realmente um Diário Oficial
            if validar_diario_oficial?(item['title'], item['link'])
              return {
                nome: item['title'],
                url: item['link'],
                snippet: item['snippet'][0..150]
              }
            end
          end
        end
      rescue => e
        puts "      ⚠️  Erro ao buscar diário: #{e.message[0..40]}"
      end
    end
    
    nil
  end
  
  def validar_diario_oficial?(title, url)
    titulo_lower = title.downcase
    url_lower = url.downcase
    
    # Verificar se contém termos de Diário Oficial
    diario_terms = ['diário', 'diario', 'jornal', 'imprensa', 'oficial', 'publicações', 'publicacoes']
    url_terms = ['diario', 'jornal', 'imprensa', 'oficial', 'publicacoes', 'publico']
    
    titulo_valido = diario_terms.any? { |term| titulo_lower.include?(term) }
    url_valido = url_terms.any? { |term| url_lower.include?(term) }
    
    titulo_valido && url_valido
  end
  
  def buscar_portarias_opm(municipio, diario_oficial)
    nome = municipio[:nome]
    portarias = []
    
    begin
      dominio = extrair_dominio(diario_oficial[:url])
      
      # Construir query com termos de portaria
      query_parts = PORTARIA_KEYWORDS.map { |keyword| "\"#{keyword}\"" }
      query = "site:#{dominio} (#{query_parts.join(' OR ')})"
      
      response = self.class.get(
        '',
        query: {
          key: GOOGLE_API_KEY,
          cx: SEARCH_ENGINE_ID,
          q: query,
          num: 10
        },
        timeout: 15
      )
      
      @queries_realizadas += 1
      
      if response.code == 200 && response.parsed_response['items']
        response.parsed_response['items'].each do |item|
          portaria = extrair_info_portaria(item)
          
          if portaria
            portarias << portaria
            puts "   📄 Portaria encontrada:"
            puts "      Título: #{portaria['titulo']}"
            puts "      Data: #{portaria['data_publicacao']}"
          end
        end
        
        if portarias.empty?
          puts "   📋 Nenhuma portaria de OPM encontrada neste Diário Oficial"
        end
      end
      
    rescue => e
      puts "      ⚠️  Erro ao buscar portarias: #{e.message[0..50]}"
    end
    
    portarias
  end
  
  def extrair_info_portaria(item)
    titulo = item['title']
    url = item['link']
    snippet = item['snippet']
    
    # Tentar extrair data do snippet ou URL
    data = extrair_data(snippet || titulo)
    
    # Tentar extrair número da portaria
    numero = extrair_numero_portaria(titulo)
    
    {
      titulo: titulo,
      url: url,
      snippet: snippet[0..250] || '',
      data_publicacao: data,
      numero_portaria: numero,
      keywords_encontradas: encontrar_keywords_portaria(snippet)
    }
  end
  
  def extrair_data(texto)
    # Padrões de data: DD/MM/YYYY, DD de mês de YYYY, YYYY-MM-DD
    datas = texto.scan(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})|(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})/)
    
    if datas.any?
      return datas.first.join('/')
    end
    
    'Data não identificada'
  end
  
  def extrair_numero_portaria(texto)
    # Buscar padrões como "Portaria 123", "Portaria nº 456", "Portaria Nº 789"
    match = texto.match(/[Pp]ortaria\s+n[°º]?\s*(\d+)/)
    
    if match
      return "#{match[0]}"
    end
    
    'Número não identificado'
  end
  
  def encontrar_keywords_portaria(texto)
    return [] unless texto
    
    keywords_encontradas = []
    PORTARIA_KEYWORDS.each do |keyword|
      if texto.downcase.include?(keyword.downcase)
        keywords_encontradas << keyword
      end
    end
    
    keywords_encontradas
  end
  
  def extrair_dominio(url)
    uri = URI.parse(url)
    uri.host
  rescue
    url
  end
  
  def registrar_resultado(municipio, diario_oficial, portarias)
    resultado = {
      'municipio' => municipio[:nome],
      'codigo_ibge' => municipio[:codigo],
      'diario_oficial' => diario_oficial,
      'portarias_encontradas' => portarias.any?,
      'quantidade_portarias' => portarias.count,
      'portarias' => portarias,
      'data_publicacao_mais_recente' => portarias.any? ? portarias.first['data_publicacao'] : nil,
      'timestamp' => Time.now.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    registrar_csv(municipio, diario_oficial, portarias)
    @resultados << resultado
  end
  
  def registrar_csv(municipio, diario_oficial, portarias)
    portarias_json = portarias.any? ? portarias.to_json : '[]'
    data_mais_recente = portarias.any? ? portarias.first['data_publicacao'] : ''
    
    @@csv_mutex.synchronize do
      linhas_existentes = []
      CSV.foreach(CSV_OUTPUT, headers: true) do |row|
        linhas_existentes << row.to_h
      end
      
      nova_posicao = linhas_existentes.count + 1
      
      CSV.open(CSV_OUTPUT, 'w') do |csv|
        csv << [
          'Posição',
          'Município',
          'Código IBGE',
          'Diário Oficial URL',
          'Portarias Encontradas',
          'Quantidade Portarias',
          'Data Publicação Mais Recente',
          'Portarias (JSON)',
          'Timestamp'
        ]
        
        linhas_existentes.each do |row|
          csv << row.values
        end
        
        csv << [
          nova_posicao,
          municipio[:nome],
          municipio[:codigo],
          diario_oficial ? diario_oficial[:url] : '',
          portarias.any?,
          portarias.count,
          data_mais_recente,
          portarias_json,
          Time.now.strftime('%Y-%m-%d %H:%M:%S')
        ]
      end
    end
  end
  
  def exibir_resumo(tempo_total)
    com_portarias = @resultados.count { |r| r['portarias_encontradas'] }
    sem_portarias = @resultados.count { |r| !r['portarias_encontradas'] }
    total_portarias = @resultados.sum { |r| r['quantidade_portarias'] }
    
    puts "\n" + "="*80
    puts "📊 RESUMO FINAL - DIÁRIO OFICIAL"
    puts "="*80
    puts "Municípios processados: #{@resultados.count}"
    puts "✅ Com Diário Oficial encontrado: #{@resultados.count { |r| r['diario_oficial'] }}"
    puts "✅ Com Portarias de OPM: #{com_portarias} (#{(com_portarias.to_f/@resultados.count*100).round(1)}%)"
    puts "❌ Sem Portarias encontradas: #{sem_portarias} (#{(sem_portarias.to_f/@resultados.count*100).round(1)}%)"
    puts "📄 Total de Portarias encontradas: #{total_portarias}"
    
    puts "\n📈 Estatísticas de API:"
    puts "Queries realizadas: #{@queries_realizadas}/#{QUERIES_POR_DIA}"
    puts "Queries restantes: #{QUERIES_POR_DIA - @queries_realizadas}"
    
    velocidade = @resultados.count / (tempo_total / 60)
    puts "\n⏱️  Tempo total: #{(tempo_total/60).round(1)}min (~#{velocidade.round(1)} municípios/min)"
    
    puts "="*80
    
    puts "\n📁 Resultados:"
    puts "  ✅ #{CSV_OUTPUT}"
    puts "  ✅ #{JSON_OUTPUT}"
    puts "  ✅ #{JSON_FORMATADO}"
    
    exportar_json
    exportar_json_formatado
    
    if LIMITE_MUNICIPIOS
      puts "\n🧪 MODO TESTE ATIVO - Limite: #{LIMITE_MUNICIPIOS}"
      puts "   Para rodar em todos os municípios: LIMITE_MUNICIPIOS = nil"
    end
  end
  
  def exportar_json
    @@json_mutex.synchronize do
      json_data = {
        'configuracao' => {
          'portaria_keywords' => PORTARIA_KEYWORDS
        },
        'resumo' => {
          'total_municipios' => @resultados.count,
          'com_portarias' => @resultados.count { |r| r['portarias_encontradas'] },
          'sem_portarias' => @resultados.count { |r| !r['portarias_encontradas'] },
          'total_portarias' => @resultados.sum { |r| r['quantidade_portarias'] },
          'data_execucao' => Time.now.strftime('%Y-%m-%d %H:%M:%S')
        },
        'dados' => @resultados
      }
      
      File.write(JSON_OUTPUT, JSON.pretty_generate(json_data))
    end
  end
  
  def exportar_json_formatado
    @@json_mutex.synchronize do
      com_portarias = @resultados.select { |r| r['portarias_encontradas'] }
      sem_portarias = @resultados.reject { |r| r['portarias_encontradas'] }
      
      json_formatado = {
        'metadata' => {
          'titulo' => 'Diário Oficial - Busca de Portarias OPM',
          'descricao' => 'Busca em Diários Oficiais de prefeituras mineiras por portarias de criação/nomeação de OPM',
          'data_execucao' => Time.now.strftime('%Y-%m-%d %H:%M:%S'),
          'estado' => 'Minas Gerais',
          'total_municipios_analisados' => @resultados.count,
          'modo_teste' => LIMITE_MUNICIPIOS ? true : false
        },
        
        'resumo_geral' => {
          'total_analisado' => @resultados.count,
          'com_diario_oficial' => @resultados.count { |r| r['diario_oficial'] },
          'com_portarias' => {
            'total' => com_portarias.count,
            'percentual' => ((com_portarias.count.to_f / @resultados.count) * 100).round(2)
          },
          'sem_portarias' => {
            'total' => sem_portarias.count,
            'percentual' => ((sem_portarias.count.to_f / @resultados.count) * 100).round(2)
          },
          'total_portarias' => @resultados.sum { |r| r['quantidade_portarias'] }
        },
        
        'municipios_com_portarias' => {
          'total' => com_portarias.count,
          'lista' => com_portarias.map do |municipio|
            {
              'municipio' => municipio['municipio'],
              'codigo_ibge' => municipio['codigo_ibge'],
              'diario_oficial_url' => municipio['diario_oficial']&.dig('url'),
              'quantidade_portarias' => municipio['quantidade_portarias'],
              'portarias' => municipio['portarias'].map do |portaria|
                {
                  'numero' => portaria['numero_portaria'],
                  'titulo' => portaria['titulo'],
                  'data_publicacao' => portaria['data_publicacao'],
                  'url' => portaria['url'],
                  'keywords' => portaria['keywords_encontradas']
                }
              end,
              'timestamp' => municipio['timestamp']
            }
          end.sort_by { |m| m['municipio'] }
        },
        
        'municipios_sem_portarias' => {
          'total' => sem_portarias.count,
          'lista' => sem_portarias.map do |municipio|
            {
              'municipio' => municipio['municipio'],
              'codigo_ibge' => municipio['codigo_ibge'],
              'diario_oficial_encontrado' => municipio['diario_oficial'] ? true : false,
              'diario_oficial_url' => municipio['diario_oficial']&.dig('url'),
              'timestamp' => municipio['timestamp']
            }
          end.sort_by { |m| m['municipio'] }
        }
      }
      
      File.write(JSON_FORMATADO, JSON.pretty_generate(json_formatado))
    end
  end
end

# ===== EXECUÇÃO =====

if __FILE__ == $0
  scanner = DiarioOficialOPMScanner.new('municipios_minas_gerais.csv')
  scanner.executar_varredura
end