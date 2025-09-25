from flask import Flask, request, send_file
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
import io
import os
import tempfile
import subprocess
import pymysql
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

CAMINHO_JSIGNPDF = r"JSignPdf.jar"

def obter_dados_medico(medico_id):
    try:
        conn = pymysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            database=os.getenv("DB_NAME"),
            connect_timeout=15
        )
        cursor = conn.cursor()
        query = "SELECT certificado_path, certificado_senha, assinatura_img_path FROM medicos WHERE id = %s"
        cursor.execute(query, (medico_id,))
        resultado = cursor.fetchone()
        cursor.close()
        conn.close()
        if resultado:
            return resultado[0], resultado[1], resultado[2]
        else:
            return None, None, None
    except Exception as e:
        print("Erro ao consultar certificado:", e)
        return None, None, None

@app.route('/api/gerar-atestado', methods=['POST'])
def gerar_atestado():
    data = request.json
    medico_id = data.get('medico_id')
    if not medico_id:
        return {'erro': 'medico_id é obrigatório'}, 400

    caminho_certificado, senha_certificado, assinatura_img_path = obter_dados_medico(medico_id)
    if not caminho_certificado or not senha_certificado:
        return {'erro': 'Certificado do médico não encontrado.'}, 400

    nome_paciente = data.get('nome_paciente', 'Paciente')
    medico_nome = data.get('medico_nome', 'Dr(a). Exemplo')
    medico_crm = data.get('medico_crm', 'CRM 000000')
    dias_afastamento = data.get('dias_afastamento', 0)
    data_emissao = data.get('data_emissao', '')

    # Cria o PDF temporário
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_pdf:
        buffer = io.BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=A4)
        pdf.setFont("Helvetica-Bold", 16)
        pdf.drawString(100, 800, "ATESTADO MÉDICO")
        pdf.setFont("Helvetica", 12)
        texto = f"Atesto que o(a) paciente {nome_paciente} necessita de {dias_afastamento} dias de afastamento."
        pdf.drawString(100, 770, texto)
        pdf.drawString(100, 750, f"Data de emissão: {data_emissao}")

        # Imagem da assinatura escaneada acima do nome/CRM
        y_linha = 160
        print("CAMINHO NO BANCO:", assinatura_img_path)
        print("Existe?", os.path.exists(assinatura_img_path))
        if assinatura_img_path and os.path.exists(assinatura_img_path) and assinatura_img_path.lower().endswith(('.png','.jpg','.jpeg')):
            pdf.drawImage(assinatura_img_path, 100, y_linha + 40, width=180, height=60, mask='auto')
        else:
            print("Não encontrou imagem de assinatura para desenhar no PDF.")

        # Nome e CRM na linha logo abaixo
        pdf.setFont("Helvetica", 11)
        pdf.drawString(100, y_linha, f"{medico_nome}    |    CRM: {medico_crm}")
        pdf.setFont("Helvetica", 10)
        pdf.drawString(100, y_linha - 15, "Assinatura digital válida conforme MP 2.200-2/2001")

        pdf.save()
        buffer.seek(0)
        temp_pdf.write(buffer.getvalue())
        temp_pdf_path = temp_pdf.name

    # Comando para chamar o JSignPdf
    comando = [
        'java', '-jar', CAMINHO_JSIGNPDF,
        '-kst', 'PKCS12',
        '-ksf', caminho_certificado,
        '-ksp', senha_certificado,
        temp_pdf_path
    ]

    # Executa o comando para assinar o PDF
    try:
        resultado = subprocess.run(comando, capture_output=True, text=True, check=True)
        print("STDOUT:", resultado.stdout)
        print("STDERR:", resultado.stderr)
    except subprocess.CalledProcessError as e:
        os.remove(temp_pdf_path)
        return {'erro': 'Erro ao assinar PDF: ' + e.stderr}, 500

    # Procura pelo arquivo assinado na pasta do projeto
    basename = os.path.basename(temp_pdf_path)
    signed_name = basename.replace('.pdf', '_signed.pdf')
    temp_pdf_assinado_path = os.path.join(os.getcwd(), signed_name)
    if not os.path.exists(temp_pdf_assinado_path):
        os.remove(temp_pdf_path)
        return {'erro': 'PDF assinado não encontrado.'}, 500

    # Lê o PDF assinado para devolver
    try:
        with open(temp_pdf_assinado_path, 'rb') as f_signed:
            signed_pdf_bytes = f_signed.read()
    except Exception:
        os.remove(temp_pdf_path)
        os.remove(temp_pdf_assinado_path)
        return {'erro': 'Não foi possível ler o PDF assinado.'}, 500

    # Limpa arquivos temporários
    os.remove(temp_pdf_path)
    os.remove(temp_pdf_assinado_path)

    # Retorna o PDF assinado
    return send_file(
        io.BytesIO(signed_pdf_bytes),
        mimetype='application/pdf',
        as_attachment=True,
        download_name='atestado_assinado.pdf'
    )

if __name__ == '__main__':
    app.run(debug=True, port=5000)
