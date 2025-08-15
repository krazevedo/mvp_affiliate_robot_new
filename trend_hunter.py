# trend_hunter.py - Versão 2.1 (Com Notificação Privada)
import os, sys, json, re, requests
from datetime import datetime
import google.generativeai as genai

# Carrega segredos
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_ADMIN_ID = os.environ.get("TELEGRAM_ADMIN_ID") # Lendo o novo segredo

# Configura IA
if not GEMINI_API_KEY: sys.exit("ERRO: GEMINI_API_KEY não encontrado.")
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash-latest')

def extrair_keywords_atuais(caminho_arquivo="keywords.txt"):
    print(f"Lendo palavras-chave de {caminho_arquivo}")
    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Erro ao ler keywords: {e}")
        return []

def gerar_sugestoes_com_ia(keywords_atuais):
    mes_atual = datetime.now().strftime('%B')
    prompt = (f"Você é um especialista em tendências de e-commerce no Brasil. Estamos em {mes_atual}. As palavras-chave atuais são: {json.dumps(keywords_atuais, ensure_ascii=False)}. Sugira 5 novas palavras-chave de produtos específicos com alto potencial de venda. Retorne APENAS um array JSON de strings. Ex: [\"câmera de segurança wifi\", \"robô aspirador de pó\"]")
    try:
        response = model.generate_content(prompt)
        texto_limpo = response.text.strip().replace("```json", "").replace("```", "")
        return json.loads(texto_limpo)
    except Exception as e:
        print(f"Erro na IA: {e}"); return None

def salvar_sugestoes(sugestoes):
    with open("sugestoes.txt", "w", encoding='utf-8') as f:
        f.write("\n".join(sugestoes))
    print(f"Sugestões salvas em sugestoes.txt")

def notificar_telegram_admin(sugestoes):
    if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_ID]):
        print("Credenciais de admin do Telegram não configuradas. Pulando notificação.")
        return

    print(f"Enviando notificação privada para o admin ID: {TELEGRAM_ADMIN_ID}")
    lista_sugestoes = "\n".join([f"- `{sugestao}`" for sugestao in sugestoes])
    mensagem = (f"🤖 *Novas Sugestões de Keywords Encontradas!*\n\nO Caçador de Tendências encontrou {len(sugestoes)} novas palavras-chave:\n\n{lista_sugestoes}\n\nElas foram salvas no arquivo `sugestoes.txt` para sua análise.")

    # Envia a mensagem para o ID do admin, não para o canal público
    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={'chat_id': TELEGRAM_ADMIN_ID, 'text': mensagem, 'parse_mode': 'Markdown'})
    print("Notificação de admin enviada com sucesso.")

def fazer_commit_das_sugestoes():
    # Adiciona um passo para fazer commit do novo arquivo de sugestões
    os.system("git config --global user.name 'github-actions[bot]'")
    os.system("git config --global user.email 'github-actions[bot]@users.noreply.github.com'")
    os.system("git add sugestoes.txt")
    # Apenas faz o commit se houver alguma mudança real no arquivo
    if os.system("git diff-index --quiet HEAD") != 0:
        os.system("git commit -m 'Adiciona/Atualiza sugestões de keywords'")
        os.system("git push")
        print("Arquivo de sugestões salvo no repositório.")
    else:
        print("Nenhuma alteração no arquivo de sugestões para salvar.")


if __name__ == "__main__":
    keywords = extrair_keywords_atuais()
    if keywords:
        sugestoes = gerar_sugestoes_com_ia(keywords)
        if sugestoes:
            salvar_sugestoes(sugestoes)
            notificar_telegram_admin(sugestoes)
            fazer_commit_das_sugestoes()