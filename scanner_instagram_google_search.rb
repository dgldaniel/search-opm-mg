# scanner_instagram_google_search.rb

require 'httparty'
require 'json'
require 'csv'
require 'uri'

class InstagramGoogleOPMScanner
  include HTTParty
  
  base_uri 'https://www.googleapis.com/customsearch/v1'
  
  # Suas credenciais
  GOOGLE_API_KEY = 'AIzaSyCns71NFE0dN9Ij8ogLqP7sJdZEsaGleyU'
  SEARCH_ENGINE_ID = '80b8acf80e0e84a18'
  
  # ⭐ LIMITE PARA TESTES
  LIMITE_MUNICIPIOS = 5
  
  # Palavras-chave para OPM
  OPM_KEYWORDS = [
    'política para mulheres',
    'secretária política das mulheres',
    'secretaria política das mulheres',
    'coordenadoria política das mulheres',
    'coordenadoria politica das mulheres',
    'diretoria política das mulheres',
    'diretoria politica das mulheres',
    'núcleo política das mulheres',
    'nucleo política das mulheres',
    'nucleo politica das mulheres',
    'violência contra mulher',
    'violencia contra mulher',
    'ação mulheres',
    'acao mulheres',
    'conselho política mulheres',
    'conselho politica mulheres',
    'sociedade civil',
    'violência contra mulheres',
    'violencia contra mulheres',
    'organismo de política para mulheres',
    'OPM'
  ].freeze
  
  # Termos de exclusão (falsos positivos)
  EXCLUSION_KEYWORDS = [
    'homem',
    'masculino',
    'patrimônio',
    'mulher moderna',
    'revista mulher',
    'filme mulher',
    'livro mulher',
    'música mulher',
    'personagem mulher',
    'dia da mulher',
    'festa mulher',
    'moda mulher',
    'beleza mulher',
    'mulheres famosas',
    'mulheres celebridades'
  ].freeze
  
  QUERIES_POR_DIA = 100
  DELAY_ENTRE_QUERIES = 0.9
  
  CSV_OUTPUT = 'instagram_google_opm_resultado.csv'
  JSON_OUTPUT = 'instagram_google_opm_resultado.json'
  JSON_FORMATADO = 'instagram_google_opm_resultado_formatado.json'
  
  @@csv_mutex = Mutex.new
  @@json_mutex = Mutex.new
  
  def initialize(csv_municipios)
    @municipios = carregar_municipios(csv_municipios)
    
    if LIMITE_MUNICIPIOS
      @municipios = @municipios.first(LIMITE_MUNICIPIOS)
    end
    
    @resultados = []
    @contas_nao_encontradas = []
    @queries_realizadas = 0
    @limite_atingido = false
    
    criar_csv_inicial
  end
  
  def executar_varredura
    modo = LIMITE_MUNICIPIOS ? "🧪 MODO TESTE (#{LIMITE_MUNICIPIOS} primeiros)" : "🔍 MODO COMPLETO"
    
    puts "#{modo} - Scanner Instagram OPM (Google Search)"
    puts "📸 Municípios a processar: #{@municipios.count}"
    puts "🔑 Palavras-chave: #{OPM_KEYWORDS.count}"
    puts "🚫 Exclusões: #{EXCLUSION_KEYWORDS.count}"
    puts "📈 Limite gratuito Google: #{QUERIES_POR_DIA} queries/dia\n\n"
    
    tempo_inicio = Time.now
    
    @municipios.each_with_index do |municipio, index|
      break if @limite_atingido
      
      puts "▶️  [#{index + 1}/#{@municipios.count}] Processando #{municipio[:nome]}..."
      
      sleep(DELAY_ENTRE_QUERIES)
      
      # ETAPA 1: Encontrar conta Instagram da prefeitura
      conta_instagram = encontrar_conta_instagram(municipio)
      
      if conta_instagram
        puts "   ✅ Instagram encontrado: #{conta_instagram[:url]}"
        
        # ETAPA 2: Buscar posts com OPM keywords no Instagram
        posts_opm = buscar_posts_opm(municipio, conta_instagram)
        
        registrar_resultado(municipio, conta_instagram, posts_opm)
      else
        puts "   ❌ Instagram oficial não encontrado"
        @contas_nao_encontradas << municipio[:nome]
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
          'Instagram Usuario',
          'Instagram URL',
          'Posts com OPM',
          'Quantidade Posts',
          'Posts (JSON)',
          'Timestamp'
        ]
      end
    end
    puts "✅ Arquivo CSV criado: #{CSV_OUTPUT}\n"
  end
  
  def encontrar_conta_instagram(municipio)
    nome = municipio[:nome]
    
    # Variações de nomes para buscar
    nomes_possíveis = [
      "prefeitura #{nome}",
      "prefeitura_#{nome.gsub(/\s+/, '_')}",
      "pref #{nome}",
      nome
    ]
    
    nomes_possíveis.each do |termo_busca|
      query = "site:instagram.com \"#{termo_busca}\" prefeitura OR municipio"
      
      begin
        response = self.class.get(
          '',
          query: {
            key: GOOGLE_API_KEY,
            cx: SEARCH_ENGINE_ID,
            q: query,
            num: 3
          },
          timeout: 15
        )
        
        @queries_realizadas += 1
        
        if response.code == 200 && response.parsed_response['items']
          response.parsed_response['items'].each do |item|
            conta = extrair_dados_instagram(item)
            
            if validar_conta_governo?(conta[:username])
              return conta
            end
          end
        end
      rescue => e
        puts "      ⚠️  Erro ao buscar: #{e.message[0..40]}"
      end
    end
    
    nil
  end
  
  def extrair_dados_instagram(item)
    # Extrair username da URL do Instagram
    # Padrão: instagram.com/username/
    url = item['link']
    match = url.match(/instagram\.com\/([a-zA-Z0-9_.]+)\/?/)
    username = match ? match[1] : 'desconhecido'
    
    {
      username: username,
      url: "https://instagram.com/#{username}",
      titulo: item['title'],
      snippet: item['snippet'][0..150]
    }
  end
  
  def validar_conta_governo?(username)
    termos_governo = ['prefeitura', 'prefeita', 'governo', 'municipio', 'municipal', 'pref', 'pmf', 'camara']
    
    username_lower = username.downcase
    
    termos_governo.any? { |termo| username_lower.include?(termo) }
  end
  
  def buscar_posts_opm(municipio, conta_instagram)
    posts_opm = []
    username = conta_instagram[:username]
    
    begin
      # Construir query para buscar posts no perfil
      query_parts = OPM_KEYWORDS.map { |keyword| "\"#{keyword}\"" }
      query = "site:instagram.com/#{username}/ (#{query_parts.join(' OR ')})"
      
      response = self.class.get(
        '',
        query: {
          key: GOOGLE_API_KEY,
          cx: SEARCH_ENGINE_ID,
          q: query,
          num: 20  # Pegar até 20 posts
        },
        timeout: 15
      )
      
      @queries_realizadas += 1
      
      if response.code == 200 && response.parsed_response['items']
        response.parsed_response['items'].each do |item|
          caption = "#{item['title']} #{item['snippet']}".downcase
          
          # Verificar se contém OPM keywords e não contém exclusion keywords
          if contem_opm_keyword?(caption) && !contem_exclusao?(caption)
            posts_opm << extrair_info_post(item)
          end
        end
        
        if posts_opm.any?
          puts "   📸 #{posts_opm.count} posts com OPM encontrados"
          posts_opm.each_with_index do |post, idx|
            puts "      #{idx + 1}. #{post['titulo']}"
            puts "         🔑 Keywords: #{post['keywords_encontradas'].join(', ')}"
          end
        else
          puts "   📸 Nenhum post com OPM encontrado"
        end
      end
      
    rescue => e
      puts "      ⚠️  Erro ao buscar posts: #{e.message[0..50]}"
    end
    
    posts_opm
  end
  
  def contem_opm_keyword?(texto)
    OPM_KEYWORDS.any? { |keyword| texto.include?(keyword.downcase) }
  end
  
  def contem_exclusao?(texto)
    EXCLUSION_KEYWORDS.any? { |keyword| texto.include?(keyword.downcase) }
  end
  
  def extrair_info_post(item)
    titulo = item['title']
    snippet = item['snippet']
    url = item['link']
    caption_completa = "#{titulo} #{snippet}"
    
    # Extrair data se disponível
    data = extrair_data_post(url)
    
    {
      titulo: titulo,
      url: url,
      snippet: snippet[0..250],
      caption: caption_completa[0..500],
      data: data,
      keywords_encontradas: encontrar_opm_keywords(caption_completa)
    }
  end
  
  def extrair_data_post(url)
    # Tentar extrair data da URL do Instagram
    # Padrão: /p/ABC123DEF/ ou /reel/ABC123DEF/
    # As URLs geralmente não contêm data legível, então usar "Data não disponível"
    'Data não disponível (via URL)'
  end
  
  def encontrar_opm_keywords(texto)
    keywords = []
    OPM_KEYWORDS.each do |keyword|
      if texto.downcase.include?(keyword.downcase)
        keywords << keyword
      end
    end
    keywords
  end
  
  def registrar_resultado(municipio, conta_instagram, posts_opm)
    resultado = {
      'municipio' => municipio[:nome],
      'codigo_ibge' => municipio[:codigo],
      'instagram_conta' => conta_instagram,
      'posts_encontrados' => posts_opm.any?,
      'quantidade_posts' => posts_opm.count,
      'posts' => posts_opm,
      'timestamp' => Time.now.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    registrar_csv(municipio, conta_instagram, posts_opm)
    @resultados << resultado
  end
  
  def registrar_csv(municipio, conta_instagram, posts_opm)
    posts_json = posts_opm.any? ? posts_opm.to_json : '[]'
    
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
          'Instagram Usuario',
          'Instagram URL',
          'Posts com OPM',
          'Quantidade Posts',
          'Posts (JSON)',
          'Timestamp'
        ]
        
        linhas_existentes.each do |row|
          csv << row.values
        end
        
        csv << [
          nova_posicao,
          municipio[:nome],
          municipio[:codigo],
          conta_instagram ? conta_instagram[:username] : '',
          conta_instagram ? conta_instagram[:url] : '',
          posts_opm.any?,
          posts_opm.count,
          posts_json,
          Time.now.strftime('%Y-%m-%d %H:%M:%S')
        ]
      end
    end
  end
  
  def exibir_resumo(tempo_total)
    com_posts = @resultados.count { |r| r['posts_encontrados'] }
    sem_posts = @resultados.count { |r| !r['posts_encontrados'] }
    total_posts = @resultados.sum { |r| r['quantidade_posts'] }
    contas_encontradas = @resultados.count { |r| r['instagram_conta'] }
    
    puts "\n" + "="*80
    puts "📊 RESUMO FINAL - INSTAGRAM (Google Search)"
    puts "="*80
    puts "Municípios processados: #{@resultados.count}"
    puts "📸 Contas Instagram encontradas: #{contas_encontradas}"
    puts "❌ Contas não encontradas: #{@contas_nao_encontradas.count}"
    puts "✅ Com posts sobre OPM: #{com_posts} (#{(com_posts.to_f/@resultados.count*100).round(1)}%)"
    puts "❌ Sem posts sobre OPM: #{sem_posts} (#{(sem_posts.to_f/@resultados.count*100).round(1)}%)"
    puts "📝 Total de posts encontrados: #{total_posts}"
    
    if @contas_nao_encontradas.any?
      puts "\n❌ Contas não encontradas:"
      @contas_nao_encontradas.each { |nome| puts "   - #{nome}" }
    end
    
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
      puts "   Para rodar em todos: LIMITE_MUNICIPIOS = nil"
    end
  end
  
  def exportar_json
    @@json_mutex.synchronize do
      json_data = {
        'configuracao' => {
          'opm_keywords' => OPM_KEYWORDS,
          'exclusion_keywords' => EXCLUSION_KEYWORDS
        },
        'resumo' => {
          'total_municipios' => @resultados.count,
          'contas_encontradas' => @resultados.count { |r| r['instagram_conta'] },
          'com_posts_opm' => @resultados.count { |r| r['posts_encontrados'] },
          'sem_posts_opm' => @resultados.count { |r| !r['posts_encontrados'] },
          'total_posts' => @resultados.sum { |r| r['quantidade_posts'] },
          'data_execucao' => Time.now.strftime('%Y-%m-%d %H:%M:%S')
        },
        'dados' => @resultados
      }
      
      File.write(JSON_OUTPUT, JSON.pretty_generate(json_data))
    end
  end
  
  def exportar_json_formatado
    @@json_mutex.synchronize do
      com_posts = @resultados.select { |r| r['posts_encontrados'] && r['instagram_conta'] }
      sem_posts = @resultados.reject { |r| r['posts_encontrados'] }
      contas_nao_encontradas = @resultados.select { |r| !r['instagram_conta'] }
      
      # Contar keywords mais frequentes
      keywords_frequencia = {}
      com_posts.each do |municipio|
        municipio['posts'].each do |post|
          post['keywords_encontradas'].each do |palavra|
            keywords_frequencia[palavra] ||= 0
            keywords_frequencia[palavra] += 1
          end
        end
      end
      
      keywords_ordenadas = keywords_frequencia.sort_by { |_, count| -count }
      
      json_formatado = {
        'metadata' => {
          'titulo' => 'Instagram OPM - Organismos de Políticas para Mulheres',
          'descricao' => 'Busca de posts em contas Instagram de prefeituras mineiras sobre OPM (via Google Search)',
          'data_execucao' => Time.now.strftime('%Y-%m-%d %H:%M:%S'),
          'estado' => 'Minas Gerais',
          'total_municipios_analisados' => @resultados.count,
          'modo_teste' => LIMITE_MUNICIPIOS ? true : false,
          'limite_municipios' => LIMITE_MUNICIPIOS
        },
        
        'resumo_geral' => {
          'total_municipios' => @resultados.count,
          'contas_instagram' => {
            'encontradas' => @resultados.count { |r| r['instagram_conta'] },
            'nao_encontradas' => contas_nao_encontradas.count,
            'percentual_encontradas' => (((@resultados.count { |r| r['instagram_conta'] }).to_f / @resultados.count) * 100).round(2)
          },
          'posts_com_opm' => {
            'total' => com_posts.count,
            'percentual' => com_posts.count > 0 ? ((com_posts.count.to_f / @resultados.count) * 100).round(2) : 0
          },
          'total_posts_opm' => @resultados.sum { |r| r['quantidade_posts'] }
        },
        
        'keywords_mais_frequentes' => keywords_ordenadas.map do |keyword, count|
          {
            'keyword' => keyword,
            'ocorrencias' => count,
            'percentual' => com_posts.count > 0 ? ((count.to_f / com_posts.count) * 100).round(2) : 0
          }
        end,
        
        'municipios_com_posts_opm' => {
          'total' => com_posts.count,
          'lista' => com_posts.map do |municipio|
            {
              'municipio' => municipio['municipio'],
              'codigo_ibge' => municipio['codigo_ibge'],
              'instagram' => {
                'username' => municipio['instagram_conta']['username'],
                'url' => municipio['instagram_conta']['url']
              },
              'quantidade_posts' => municipio['quantidade_posts'],
              'posts' => municipio['posts'].map do |post|
                {
                  'titulo' => post['titulo'],
                  'url' => post['url'],
                  'snippet' => post['snippet'],
                  'keywords' => post['keywords_encontradas']
                }
              end,
              'timestamp' => municipio['timestamp']
            }
          end.sort_by { |m| m['municipio'] }
        },
        
        'municipios_sem_posts_opm' => {
          'total' => sem_posts.count,
          'lista' => sem_posts.select { |r| r['instagram_conta'] }.map do |municipio|
            {
              'municipio' => municipio['municipio'],
              'codigo_ibge' => municipio['codigo_ibge'],
              'instagram' => municipio['instagram_conta'] ? municipio['instagram_conta']['url'] : nil,
              'timestamp' => municipio['timestamp']
            }
          end.sort_by { |m| m['municipio'] }
        },
        
        'contas_nao_encontradas' => {
          'total' => contas_nao_encontradas.count,
          'lista' => contas_nao_encontradas.map do |municipio|
            {
              'municipio' => municipio['municipio'],
              'codigo_ibge' => municipio['codigo_ibge']
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
  scanner = InstagramGoogleOPMScanner.new('municipios_minas_gerais.csv')
  scanner.executar_varredura
end