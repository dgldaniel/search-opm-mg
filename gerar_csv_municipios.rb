require 'net/http'
require 'json'
require 'csv'

# URL da API do IBGE
url = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados/MG/municipios'

# Fazer a requisição HTTP
uri = URI(url)
response = Net::HTTP.get_response(uri)

# Verificar se a requisição foi bem-sucedida
if response.code.to_i == 200
  # Parsear o JSON
  municipios = JSON.parse(response.body)
  
  # Criar o arquivo CSV
  CSV.open('municipios_minas_gerais.csv', 'w', headers: true) do |csv|
    # Adicionar cabeçalhos
    csv << ['nome', 'codigo_ibge']
    
    # Adicionar dados dos municípios
    municipios.each do |municipio|
      csv << [municipio['nome'], municipio['id']]
    end
  end
  
  puts "✅ Arquivo 'municipios_minas_gerais.csv' criado com sucesso!"
  puts "📊 Total de municípios: #{municipios.length}"
else
  puts "❌ Erro ao buscar dados da API: #{response.code}"
end