import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine
from datetime import date
import urllib.parse

# Configurar o serviço do WebDriver
navegador = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# Definir a URL e as credenciais
url = 'https://onecxford.medallia.com/sso/onecxford/applications/ex_WEB-9/pages/2258?roleId=88523&f.calculation=29&f.columns=tp&f.benchmark=100000004&f.pfk_ford_onecx_journey_segment_alt=191_2&f.pfe_ford_dealer_name_unit=1093416&f.timeperiod=364&f.reporting-date=k_onecx_cvp_final_posted_datetime&alreftoken=%22%5C%2291a4c8c6753893074be0d8f77de14270%5C%22%5C%22%22'
login = "A-jun161@b2bford.com"
senha = "America@2025"

# Acessar a URL
navegador.get(url)

# Esperar até que a página seja carregada e o elemento esteja disponível
wait = WebDriverWait(navegador, 20)

# Clique dealer
dealer = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div.idp:nth-child(5) > div:nth-child(2) > span:nth-child(1)")))
dealer.click()

# Campo login
campo_login = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="userName"]')))
campo_login.send_keys(login)

# Campo senha
campo_senha = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="password"]')))
campo_senha.send_keys(senha)

# Clique para logar
click_login = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '#btn-sign-in')))
click_login.click()

# Clicar em concessionária
click_filtro = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[3]/div[2]/div/main/div[1]/div[1]/div/div/div/div/div/div/div/div[1]/button/div/div[2]')))
click_filtro.click()

click_servico = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[4]/div[2]/div/div[2]/div/div[2]/ul/li[4]/div[1]')))
click_servico.click()

# Esperar carregamento dos dados
time.sleep(10)

# Coletar os dados
NPS = navegador.find_element(By.XPATH, '/html/body/div[3]/div[2]/div/main/div[2]/div/div[1]/div/div[1]/div[2]/div[2]/div/section/div/div[1]/div/div[1]/div[1]/div[2]/span').text
NPS_Nacional = navegador.find_element(By.XPATH, '/html/body/div[3]/div[2]/div/main/div[2]/div/div[1]/div/div[1]/div[2]/div[2]/div/section/div/div[2]').text
Satisfacao = navegador.find_element(By.XPATH, '/html/body/div[3]/div[2]/div/main/div[2]/div/div[1]/div/div[2]/div[2]/div[2]/div/section/div/div[1]/div/div[1]/div[1]/div[2]/span').text
Satisfacao_Nacional = navegador.find_element(By.XPATH, '/html/body/div[3]/div[2]/div/main/div[2]/div/div[1]/div/div[2]/div[2]/div[2]/div/section/div/div[2]').text
Indice = navegador.find_element(By.XPATH, '/html/body/div[3]/div[2]/div/main/div[2]/div/div[1]/div/div[4]/div[2]/div[2]/div/section/div/div[1]/div/div[1]/div[1]/div[1]/span').text
Indice_Nacional = navegador.find_element(By.XPATH, '/html/body/div[3]/div[2]/div/main/div[2]/div/div[1]/div/div[4]/div[2]/div[2]/div/section/div/div[2]').text

# Criar DataFrame
df = pd.DataFrame({
    "Nota": [NPS],
    "Nota_Nacional": [NPS_Nacional],
    "Satisfacao": [Satisfacao],
    "Satisfacao_Nacional": [Satisfacao_Nacional],
    "Indice": [Indice],
    "Indice_Nacional": [Indice_Nacional]
})

# Limpeza de texto
df['Nota_Nacional'] = df['Nota_Nacional'].str.replace('País da Concessionária: ', '', regex=False)
df['Satisfacao_Nacional'] = df['Satisfacao_Nacional'].str.replace('País da Concessionária: ', '', regex=False)
df['Indice_Nacional'] = df['Indice_Nacional'].str.replace('País da Concessionária: ', '', regex=False)

df['Satisfacao_Global'] = 'Ford'

# Adicionar coluna segmento e data
df['Segmento'] = 'Pos Vendas'
df['data_atualizacao'] = date.today()

# Conexão com banco de dados SQL Server
print("Conectando ao banco de dados...")
user = 'rpa_bi'
password = 'Rp@_B&_P@rvi'
host = '10.0.10.243'
port = '54949'
database = 'stage'

params = urllib.parse.quote_plus(
    f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={host},{port};DATABASE={database};UID={user};PWD={password}')
connection_str = f'mssql+pyodbc:///?odbc_connect={params}'
engine = create_engine(connection_str)
table_name = "QualidadeFord"

# Inserir dados no banco
with engine.connect() as connection:
    df.to_sql(table_name, con=connection, if_exists='replace', index=False)

print(f"Dados inseridos com sucesso na tabela '{table_name}'!")

# Fechando o navegador
time.sleep(10)
print("Fechando o navegador...")
navegador.quit()
print("Navegador fechado com sucesso.")

# Fechar conexão com o banco
engine.dispose()
print("Conexão com o banco encerrada.")
