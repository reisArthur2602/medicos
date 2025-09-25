import os
import tempfile
from pdf2image import convert_from_path
from PIL import Image
import qrcode
from io import BytesIO
import pymysql
import pytz
from datetime import datetime

TZ = pytz.timezone('America/Sao_Paulo')

def obter_dados_medico(medico_id):
    import os
    try:
        conn = pymysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            database=os.getenv("DB_NAME"),
            connect_timeout=15
        )
        cursor = conn.cursor()
        query = """
            SELECT certificado_path, certificado_senha, assinatura_img_path, crm, nome, papel_timbrado_path_a4, papel_timbrado_path_a5
            FROM medicos WHERE id = %s
        """
        cursor.execute(query, (medico_id,))
        resultado = cursor.fetchone()
        cursor.close()
        conn.close()
        if resultado:
            return (
                resultado[0].strip() if resultado[0] else None,   # certificado_path
                resultado[1].strip() if resultado[1] else None,   # certificado_senha
                resultado[2].strip() if resultado[2] else None,   # assinatura_img_path
                resultado[3].strip() if resultado[3] else '',     # crm
                resultado[4].strip() if resultado[4] else '',     # nome
                resultado[5].strip() if resultado[5] else None,   # papel_timbrado_path_a4
                resultado[6].strip() if resultado[6] else None    # papel_timbrado_path_a5
            )
        else:
            return (None,) * 7
    except Exception as e:
        print("Erro ao consultar certificado:", e)
        return (None,) * 7

def gerar_qrcode(url):
    qr = qrcode.QRCode(box_size=3, border=1)
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = BytesIO()
    img.save(buffer)
    buffer.seek(0)
    return buffer

def desenhar_fundo_papel(pdf, papel_timbrado_path, largura, altura):
    if not papel_timbrado_path:
        return
    ext = os.path.splitext(papel_timbrado_path)[1].lower()
    try:
        if ext == ".pdf":
            paginas = convert_from_path(papel_timbrado_path, dpi=300, size=(int(largura), int(altura)))
            img_bg = paginas[0]
            temp_bg_path = os.path.join(tempfile.gettempdir(), "tmp_papel_timbrado.png")
            img_bg.save(temp_bg_path, format="PNG", quality=95)
            pdf.drawImage(temp_bg_path, 0, 0, width=largura, height=altura, mask='auto')
            os.remove(temp_bg_path)
        elif ext in [".png", ".jpg", ".jpeg"]:
            with Image.open(papel_timbrado_path) as bg:
                bg = bg.convert("RGB")
                bg = bg.resize((int(largura), int(altura)), Image.LANCZOS)
                temp_bg_path = os.path.join(tempfile.gettempdir(), "tmp_papel_timbrado.png")
                bg.save(temp_bg_path, quality=95)
                pdf.drawImage(temp_bg_path, 0, 0, width=largura, height=altura, mask='auto')
                os.remove(temp_bg_path)
    except Exception as e:
        print(f"Erro ao processar fundo do papel timbrado: {e}")

def desenhar_texto_multilinha(pdf, texto, x, y, largura_caixa, fontname="Helvetica", fontsize=11, leading=14):
    from reportlab.pdfbase.pdfmetrics import stringWidth
    linhas = []
    palavras = texto.split(' ')
    linha_atual = ""
    for palavra in palavras:
        test_line = linha_atual + (" " if linha_atual else "") + palavra
        if stringWidth(test_line, fontname, fontsize) < largura_caixa:
            linha_atual = test_line
        else:
            linhas.append(linha_atual)
            linha_atual = palavra
    if linha_atual:
        linhas.append(linha_atual)
    for linha in linhas:
        pdf.drawString(x, y, linha)
        y -= leading
    return y

def get_or_create_paciente(conn, nome, cpf, data_nascimento, sexo):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM pacientes WHERE cpf = %s", (cpf,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "INSERT INTO pacientes (nome, cpf, data_nascimento, sexo, criado_em) VALUES (%s, %s, %s, %s, %s)",
        (nome, cpf, data_nascimento, sexo, datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S'))
    )
    conn.commit()
    return cursor.lastrowid
