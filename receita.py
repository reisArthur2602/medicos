from flask import Blueprint, request, send_file, send_from_directory, render_template
from reportlab.lib.pagesizes import A4, A5
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
import io, os, tempfile, subprocess, pymysql, base64, glob
from dotenv import load_dotenv
from PIL import Image
from datetime import datetime
from pdf2image import convert_from_path
import qrcode
from io import BytesIO
import pytz

# PyPDF2 para overlay do bloco digital após assinar
from PyPDF2 import PdfReader, PdfWriter
try:
    from PyPDF2 import Transformation
except Exception:
    Transformation = None

load_dotenv()

receita_bp = Blueprint('receita', __name__)

TZ = pytz.timezone('America/Sao_Paulo')
os.environ['TZ'] = 'America/Sao_Paulo'

CAMINHO_JSIGNPDF = os.getenv("JSIGNPDF_JAR", r"JSignPdf.jar")
PUBLIC_BASE_URL = (os.getenv("PUBLIC_BASE_URL") or "").rstrip("/")
PASTA_RECEITAS = os.path.join(os.getcwd(), "receitas")
os.makedirs(PASTA_RECEITAS, exist_ok=True)

# -------------------- DB helpers --------------------
def _db():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        connect_timeout=15,
        charset="utf8mb4",
        autocommit=True
    )

def _table_exists(cur, table):
    try:
        cur.execute("SHOW TABLES LIKE %s", (table,))
        return cur.fetchone() is not None
    except Exception:
        return False

# -------------------- util/format helpers --------------------
def _clean(s):
    if s is None:
        return None
    return str(s).strip().replace('\x00', '')

def _clean_path(p):
    if not p:
        return None
    return str(p).strip().strip('"').replace('\r', '').replace('\n', '')

def fmt_cpf(cpf):
    s = ''.join(ch for ch in str(cpf or '') if ch.isdigit())
    if len(s) == 11:
        return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}"
    return str(cpf or '')

def fmt_data(d):
    """dd/mm/aaaa a partir de ISO, YYYY-MM-DD, YYYYMMDD, etc."""
    if not d:
        return ''
    s = str(d).strip()
    try:
        s2 = s.replace('Z', '+00:00')
        dt = datetime.fromisoformat(s2[:26])
        return dt.strftime('%d/%m/%Y')
    except Exception:
        pass
    if len(s) >= 10 and s[4] == '-' and s[7] == '-':
        y, m, d2 = s[:4], s[5:7], s[8:10]
        return f"{d2}/{m}/{y}"
    if len(s) == 8 and s.isdigit():
        y, m, d2 = s[:4], s[4:6], s[6:8]
        return f"{d2}/{m}/{y}"
    return s

def norm_date_sql(d):
    """Para salvar YYYY-MM-DD no DB."""
    if not d:
        return None
    s = str(d).strip()
    try:
        s2 = s.replace('Z', '+00:00')
        dt = datetime.fromisoformat(s2[:26])
        return dt.strftime('%Y-%m-%d')
    except Exception:
        pass
    if len(s) >= 10 and s[4] == '-' and s[7] == '-':
        return s[:10]
    if len(s) == 10 and s[2] == '/' and s[5] == '/':
        d2, m, y = s[:2], s[3:5], s[6:10]
        return f"{y}-{m}-{d2}"
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s

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
    ext = (os.path.splitext(papel_timbrado_path)[1] or "").lower()
    try:
        if ext == ".pdf":
            paginas = convert_from_path(papel_timbrado_path, dpi=300, size=(int(largura), int(altura)))
            img_bg = paginas[0]
            temp_bg_path = os.path.join(tempfile.gettempdir(), "tmp_papel_timbrado.png")
            img_bg.save(temp_bg_path, format="PNG", quality=95)
            pdf.drawImage(temp_bg_path, 0, 0, width=largura, height=altura, mask='auto')
            os.remove(temp_bg_path)
        elif ext in (".png", ".jpg", ".jpeg"):
            with Image.open(papel_timbrado_path) as bg:
                bg = bg.convert("RGB").resize((int(largura), int(altura)), Image.LANCZOS)
                temp_bg_path = os.path.join(tempfile.gettempdir(), "tmp_papel_timbrado.png")
                bg.save(temp_bg_path, quality=95)
                pdf.drawImage(temp_bg_path, 0, 0, width=largura, height=altura, mask='auto')
                os.remove(temp_bg_path)
    except Exception as e:
        print(f"Erro ao processar fundo do papel timbrado: {e}")

# ------------------------------------------------------------
# Layout dinâmico A4/A5
# ------------------------------------------------------------
def get_layout_params(tamanho_papel, largura, altura):
    is_a4 = (tamanho_papel == 'A4')
    return {
        "margem_x": 50 if is_a4 else 25,
        "title_font": 14 if is_a4 else 13,
        "body_font": 11 if is_a4 else 10,
        "small_font": 10 if is_a4 else 9,
        "gap_line": 18 if is_a4 else 16,
        "leading": 15 if is_a4 else 13,
        "presc_label_gap": 18 if is_a4 else 16,
        "via1_box_height": 200 if is_a4 else 170,
        "via2_presc_lines": 7 if is_a4 else 5,
        "via2_presc_gap": 16 if is_a4 else 14,
        "via2_box_height": 180 if is_a4 else 150,
        "assin_largura": 320 if is_a4 else 260,
        "assin_y_min": 90 if is_a4 else 80
    }

# ------------------------------------------------------------
# Texto: preserva ENTERs, linhas em branco e faz quebra por largura
# ------------------------------------------------------------
def desenhar_texto_multilinha(pdf, texto, x, y, largura_caixa,
                              fontname="Helvetica", fontsize=11, leading=15):
    from reportlab.pdfbase.pdfmetrics import stringWidth
    texto = (texto or "").replace('\r\n', '\n').replace('\r', '\n')
    blocos = texto.split('\n')
    for bloco in blocos:
        if bloco.strip() == "":
            y -= leading
            continue
        palavras = bloco.split(' ')
        atual = ""
        for palavra in palavras:
            teste = (atual + " " if atual else "") + palavra
            if stringWidth(teste, fontname, fontsize) < largura_caixa:
                atual = teste
            else:
                pdf.drawString(x, y, atual)
                y -= leading
                atual = palavra
        if atual:
            pdf.drawString(x, y, atual)
            y -= leading
    return y

# -------------------- dados do médico / papel / timbrado --------------------
def obter_dados_medico_basico(medico_id: int):
    """Retorna: cert_path, cert_senha, assinatura_img_path, crm, nome"""
    try:
        conn = _db()
        cur = conn.cursor()
        cur.execute("""
            SELECT COALESCE(certificado_path,''), COALESCE(certificado_senha,''),
                   COALESCE(assinatura_img_path,''), COALESCE(crm,''), COALESCE(nome,'')
            FROM medicos WHERE id=%s
        """, (medico_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return (None, None, None, '', '')
        cert, senha, assin, crm, nome = map(_clean, row)
        return (_clean_path(cert), (senha or '').strip(), _clean_path(assin), crm or '', nome or '')
    except Exception as e:
        print("Erro obter_dados_medico_basico:", e)
        return (None, None, None, '', '')

def obter_timbrados(medico_id: int):
    paths = {'A4': None, 'A5': None}
    try:
        conn = _db()
        cur = conn.cursor()
        if _table_exists(cur, "papeis_timbrados"):
            cur.execute("""
                SELECT tamanho, caminho
                FROM papeis_timbrados
                WHERE medico_id=%s AND ativo=1
            """, (medico_id,))
            for t, c in cur.fetchall() or []:
                tt = (t or '').upper()
                if tt in ('A4', 'A5'):
                    paths[tt] = _clean_path(c)
        cur.close()
        conn.close()
    except Exception as e:
        print("Erro obter_timbrados:", e)
    return paths

def resolver_papel_receita(medico_id: int) -> str:
    try:
        conn = _db()
        cur = conn.cursor()
        if _table_exists(cur, "preferencias_papel_medico"):
            try:
                cur.execute("""
                    SELECT tamanho_padrao
                    FROM preferencias_papel_medico
                    WHERE medico_id=%s AND UPPER(doc_tipo)='RECEITA'
                    LIMIT 1
                """, (medico_id,))
                r = cur.fetchone()
                if r and r[0] and str(r[0]).upper() in ('A4', 'A5'):
                    v = str(r[0]).upper()
                    cur.close()
                    conn.close()
                    return v
            except Exception:
                pass
        if _table_exists(cur, "clinica_config"):
            for k in ("DEFAULT_PAPER_RECEITA", "PAPER_RECEITA", "paper_receita"):
                try:
                    cur.execute("SELECT valor FROM clinica_config WHERE chave=%s LIMIT 1", (k,))
                    row = cur.fetchone()
                    if row and row[0] and str(row[0]).strip().upper() in ('A4', 'A5'):
                        v = str(row[0]).strip().upper()
                        cur.close()
                        conn.close()
                        return v
                except Exception:
                    pass
        cur.close()
        conn.close()
    except Exception:
        pass
    v_env = (os.getenv("DEFAULT_PAPER_RECEITA") or "A4").strip().upper()
    return v_env if v_env in ("A4", "A5") else "A4"

def montar_conselho_label(medico_id: int, crm_fallback: str = "") -> str:
    try:
        conn = _db()
        cur = conn.cursor()
        cur.execute("""
            SELECT tipo, codigo, uf
            FROM conselho
            WHERE medico_id=%s
            ORDER BY id DESC
            LIMIT 1
        """, (medico_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            tipo = (row[0] or '').strip().upper()
            codigo = (row[1] or '').strip()
            uf = (row[2] or '').strip().upper()
            if tipo and codigo:
                return f"{tipo}-{uf} {codigo}" if uf else f"{tipo} {codigo}"
    except Exception:
        pass
    crm_fallback = (crm_fallback or '').strip()
    return f"CRM {crm_fallback}" if crm_fallback else "Registro profissional"

# -------------------- endpoints --------------------
@receita_bp.route('/api/gerar-receita', methods=['POST'])
def gerar_receita():
    data = request.get_json(force=True, silent=True) or {}
    medico_id = data.get('medico_id')
    if not medico_id:
        return {'erro': 'medico_id é obrigatório'}, 400
    medico_id = int(medico_id)

    cert_path, cert_senha, assinatura_img_path, crm_medico, nome_medico = obter_dados_medico_basico(medico_id)
    timbrados = obter_timbrados(medico_id)
    conselho_label = montar_conselho_label(medico_id, crm_medico)

    tamanho_papel = resolver_papel_receita(medico_id)
    pagesize = A4 if tamanho_papel == 'A4' else A5
    largura, altura = pagesize
    papel_timbrado_path = timbrados.get(tamanho_papel)

    LP = get_layout_params(tamanho_papel, largura, altura)

    # Dados do paciente / conteúdo
    nome_paciente = _clean(data.get('nome_paciente') or 'Paciente')
    cpf_paciente = fmt_cpf(data.get('cpf_paciente') or '')
    data_nasc_in = _clean(data.get('data_nascimento') or '')
    nasc_fmt = fmt_data(data_nasc_in)
    nasc_sql = norm_date_sql(data_nasc_in)
    sexo = _clean(data.get('sexo') or '')
    receita_texto = data.get('receita_texto', '') or ''
    receita_controlada = bool(data.get('receita_controlada', False))

    data_emissao_dt = datetime.now(TZ)
    data_emissao = data_emissao_dt.strftime('%d/%m/%Y')

    # Cria registro
    try:
        conn = _db()
        paciente_id = _get_or_create_paciente(conn, nome_paciente, cpf_paciente, nasc_sql, sexo)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO receitas (medico_id, paciente_id, texto, data_emissao,
                                  pdf_assinado_path, assinado_em, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            medico_id, int(paciente_id), receita_texto,
            data_emissao_dt.strftime('%Y-%m-%d'),
            "TEMP", data_emissao_dt.strftime('%Y-%m-%d %H:%M:%S'), 1
        ))
        receita_id = cur.lastrowid
        cur.close()
        conn.close()
    except Exception as e:
        return {'erro': f'Falha ao salvar no banco: {e}'}, 500

    import uuid
    nome_arquivo = f"receita_{uuid.uuid4()}.pdf"
    caminho_arquivo = os.path.join(PASTA_RECEITAS, nome_arquivo)

    # ----- 1) PDF base -----
    base_fd, base_path = tempfile.mkstemp(suffix=".pdf")
    os.close(base_fd)
    try:
        buffer = io.BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=pagesize)

        # fundo
        desenhar_fundo_papel(pdf, papel_timbrado_path, largura, altura)

        # helpers visuais VIA 2
        def _linha(pdf_canvas, x, y, w, lw=0.8):
            pdf_canvas.setLineWidth(lw)
            pdf_canvas.line(x, y, x + w, y)

        def _rotulo_e_linha(pdf_canvas, x, y, texto, w, dx=70, lh=16):
            pdf_canvas.setFont("Helvetica", LP["small_font"])
            pdf_canvas.drawString(x, y, texto)
            _linha(pdf_canvas, x + dx, y - 2, w - dx)
            return y - lh

        # ----- header -----
        def draw_header(pdf_canvas, via_label=None):
            margem_x = LP["margem_x"]
            pdf_canvas.setFont("Helvetica-Bold", LP["title_font"])
            titulo_base = "RECEITUÁRIO DE CONTROLE ESPECIAL" if receita_controlada else "RECEITA MÉDICA"
            titulo = titulo_base if not via_label else f"{titulo_base} ({via_label})"
            pdf_canvas.drawString(margem_x, altura - 60, titulo)

            top_y = altura - 90
            lines = [("Paciente", f"Paciente: {nome_paciente}")]
            if cpf_paciente:
                lines.append(("CPF", f"CPF: {cpf_paciente}"))
            if nasc_fmt:
                lines.append(("Nascimento", f"Data de nascimento: {nasc_fmt}"))

            pdf_canvas.setFont("Helvetica", LP["body_font"])
            yy = top_y
            for _, text in lines:
                pdf_canvas.drawString(margem_x, yy, text)
                yy -= LP["gap_line"]
            end_y = yy - 24
            return end_y

        # ----- quadro VIA 1 -----
        def draw_quadro_receita_controlada(pdf_canvas, x, y, largura_total):
            altura_box = LP["via1_box_height"]
            pdf_canvas.setLineWidth(1)
            pdf_canvas.rect(x, y - altura_box, largura_total, altura_box)

            pdf_canvas.setFont("Helvetica-Bold", LP["body_font"])
            pdf_canvas.drawString(x + 10, y - 20, "IDENTIFICAÇÃO DO COMPRADOR")
            pdf_canvas.setFont("Helvetica", LP["small_font"])
            pdf_canvas.drawString(x + 20, y - 40, "Nome:")
            pdf_canvas.drawString(x + 20, y - 60, "Identidade:")
            pdf_canvas.drawString(x + 20, y - 80, "Endereço:")
            pdf_canvas.drawString(x + 20, y - 100, "Cidade:")
            pdf_canvas.drawString(x + 20, y - 120, "Telefone:")

            pdf_canvas.setFont("Helvetica-Bold", LP["body_font"])
            pdf_canvas.drawString(x + 10, y - 140, "IDENTIFICAÇÃO DO FORNECEDOR (FARMÁCIA)")
            pdf_canvas.setFont("Helvetica", LP["small_font"])
            pdf_canvas.drawString(x + 20, y - 160, "Nome:")
            pdf_canvas.drawString(x + 200, y - 160, "Cidade:")
            pdf_canvas.drawString(x + 20, y - 180, "CNPJ:")
            pdf_canvas.drawString(x + 200, y - 180, "Telefone:")

            pdf_canvas.setFont("Helvetica-Bold", LP["body_font"])
            pdf_canvas.drawString(x + largura_total - 220, y - 20, "FARMACÊUTICO RESPONSÁVEL")
            pdf_canvas.setFont("Helvetica", LP["small_font"])
            pdf_canvas.drawString(x + largura_total - 210, y - 40, "Nome:")
            pdf_canvas.drawString(x + largura_total - 210, y - 60, "CRF:")
            pdf_canvas.drawString(x + largura_total - 210, y - 80, "Assinatura:")
            pdf_canvas.drawString(x + largura_total - 210, y - 100, "Data:")

            return y - altura_box - 20

        # ----- body padrão (VIA 1) -----
        def draw_body(pdf_canvas, start_y, via_label=None):
            margem_x = LP["margem_x"]
            largura_texto = largura - (2 * margem_x)
            y = start_y

            pdf_canvas.setFont("Helvetica-Bold", LP["body_font"])
            pdf_canvas.drawString(margem_x, y, "Prescrição:")
            y -= LP["presc_label_gap"]

            pdf_canvas.setFont("Helvetica", LP["body_font"])
            y = desenhar_texto_multilinha(pdf_canvas, receita_texto, margem_x, y, largura_texto,
                                          fontname="Helvetica", fontsize=LP["body_font"], leading=LP["leading"])
            y -= 10

            pdf_canvas.setFont("Helvetica", LP["small_font"])
            pdf_canvas.drawString(margem_x, y, f"Data de emissão: {data_emissao}")
            y -= 25

            if receita_controlada and (via_label == "Via: 1"):
                largura_quadro = largura - (2 * margem_x)
                y = draw_quadro_receita_controlada(pdf_canvas, margem_x, y, largura_quadro)
                y -= 10

            pdf_canvas.setLineWidth(1)
            cx = largura / 2.0
            linha_w = LP["assin_largura"]
            y_linha = max(LP["assin_y_min"], y - 40)
            pdf_canvas.line(cx - linha_w / 2, y_linha, cx + linha_w / 2, y_linha)
            pdf_canvas.setFont("Helvetica", LP["small_font"])
            pdf_canvas.drawCentredString(cx, y_linha - 12, "Assinatura e carimbo do médico")

            return y_linha - 24

        # ----- body VIA 2 (modelo da foto) -----
        def draw_body_controlada_via2(pdf_canvas, start_y):
            margem_x = LP["margem_x"]
            largura_texto = largura - (2 * margem_x)
            y = start_y

            pdf_canvas.setFont("Helvetica", LP["small_font"])
            y = _rotulo_e_linha(pdf_canvas, margem_x, y, "Paciente:", largura_texto, dx=70, lh=18 if tamanho_papel == 'A4' else 16)
            y = _rotulo_e_linha(pdf_canvas, margem_x, y, "Endereço:", largura_texto, dx=70, lh=18 if tamanho_papel == 'A4' else 16)

            pdf_canvas.setFont("Helvetica", LP["small_font"])
            pdf_canvas.drawString(margem_x, y, "Prescrição:")
            y -= 12
            for _ in range(LP["via2_presc_lines"]):
                _linha(pdf_canvas, margem_x, y, largura_texto, lw=0.6)
                y -= LP["via2_presc_gap"]

            pdf_canvas.setFont("Helvetica", LP["small_font"])
            pdf_canvas.drawString(margem_x, y, f"Data: {data_emissao}")
            cxr = largura - margem_x
            _linha(pdf_canvas, cxr - (240 if tamanho_papel == 'A4' else 210), y - 2, 230 if tamanho_papel == 'A4' else 200)
            pdf_canvas.drawRightString(cxr, y - 16, "Assinatura do Médico / CRM")
            y -= 26

            col_gap = 18
            col_w = (largura_texto - col_gap) / 2.0
            left_x = margem_x
            right_x = margem_x + col_w + col_gap
            top_y = y

            box_h = LP["via2_box_height"]
            pdf_canvas.setLineWidth(1)
            pdf_canvas.rect(left_x, top_y - box_h, col_w, box_h)
            pdf_canvas.rect(right_x, top_y - box_h, col_w, box_h)

            yy = top_y - 18
            pdf_canvas.setFont("Helvetica-Bold", LP["body_font"])
            pdf_canvas.drawString(left_x + 10, yy, "IDENTIFICAÇÃO DO COMPRADOR")
            yy -= 18
            pdf_canvas.setFont("Helvetica", LP["small_font"])
            yy = _rotulo_e_linha(pdf_canvas, left_x + 10, yy, "Nome:", col_w - 20, dx=50)
            pdf_canvas.drawString(left_x + 10, yy, "Identidade:")
            _linha(pdf_canvas, left_x + 10 + 60, yy - 2, 110 if tamanho_papel == 'A4' else 95)
            pdf_canvas.drawString(left_x + 10 + 60 + (120 if tamanho_papel == 'A4' else 105), yy, "Órg.Em.:")
            _linha(pdf_canvas, left_x + 10 + 60 + (120 if tamanho_papel == 'A4' else 105) + 55, yy - 2,
                   col_w - (10 + 60 + (120 if tamanho_papel == 'A4' else 105) + 55 + 10))
            yy -= 18
            yy = _rotulo_e_linha(pdf_canvas, left_x + 10, yy, "Endereço:", col_w - 20, dx=60)
            pdf_canvas.drawString(left_x + 10, yy, "Cidade:")
            _linha(pdf_canvas, left_x + 10 + 45, yy - 2, 140 if tamanho_papel == 'A4' else 120)
            pdf_canvas.drawString(left_x + 10 + 45 + (150 if tamanho_papel == 'A4' else 130), yy, "UF:")
            _linha(pdf_canvas, left_x + 10 + 45 + (150 if tamanho_papel == 'A4' else 130) + 18, yy - 2, 28)
            yy -= 18
            yy = _rotulo_e_linha(pdf_canvas, left_x + 10, yy, "Telefone:", col_w - 20, dx=60)

            yy2 = top_y - 18
            pdf_canvas.setFont("Helvetica-Bold", LP["body_font"])
            pdf_canvas.drawString(right_x + 10, yy2, "IDENTIFICAÇÃO DO FORNECEDOR")
            yy2 -= 18
            pdf_canvas.setFont("Helvetica", LP["small_font"])
            yy2 = _rotulo_e_linha(pdf_canvas, right_x + 10, yy2, "Nome:", col_w - 20, dx=50)
            yy2 = _rotulo_e_linha(pdf_canvas, right_x + 10, yy2, "CNPJ:", col_w - 20, dx=50)
            yy2 = _rotulo_e_linha(pdf_canvas, right_x + 10, yy2, "Endereço:", col_w - 20, dx=60)
            yy2 = _rotulo_e_linha(pdf_canvas, right_x + 10, yy2, "Cidade:", col_w - 20, dx=50)
            yy2 = _rotulo_e_linha(pdf_canvas, right_x + 10, yy2, "Telefone:", col_w - 20, dx=60)

            pdf_canvas.drawString(right_x + 10, yy2, "Data:")
            _linha(pdf_canvas, right_x + 10 + 35, yy2 - 2, 80)

            _linha(pdf_canvas, right_x + 10, (top_y - box_h) + 18, col_w - 20)
            pdf_canvas.drawString(right_x + 10, (top_y - box_h) + 4, "Assinatura do Farmacêutico")

            return (top_y - box_h) - 16

        # -------- VIA 1
        y_start = draw_header(pdf, via_label=("Via: 1" if receita_controlada else None))
        draw_body(pdf, y_start, via_label=("Via: 1" if receita_controlada else None))

        # -------- VIA 2 (só quando controlada)
        if receita_controlada:
            pdf.showPage()
            desenhar_fundo_papel(pdf, papel_timbrado_path, largura, altura)
            y_start = draw_header(pdf, via_label="Via: 2")
            draw_body_controlada_via2(pdf, y_start)

        pdf.save()
        buffer.seek(0)
        with open(base_path, 'wb') as f:
            f.write(buffer.read())
    except Exception as e:
        try:
            os.remove(base_path)
        except Exception:
            pass
        return {'erro': f'Falha ao gerar PDF base: {e}'}, 500

    # ----- 2) Assinar digitalmente (invisível) -----
    assinou = False
    final_path = base_path
    has_cert_inputs = bool(cert_path and os.path.isfile(cert_path) and (cert_senha or "").strip())
    if has_cert_inputs:
        try:
            cmd = [
                'java', '-jar', CAMINHO_JSIGNPDF,
                '-kst', 'PKCS12',
                '-ksf', cert_path,
                '-ksp', cert_senha,
                base_path
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode == 0:
                base = os.path.splitext(os.path.basename(base_path))[0]
                candidates = [
                    os.path.join(os.path.dirname(base_path), base + "_signed.pdf"),
                    os.path.join(os.getcwd(), base + "_signed.pdf"),
                ]
                if not any(os.path.exists(p) for p in candidates):
                    candidates = glob.glob(os.path.join(os.path.dirname(base_path), "*_signed.pdf")) + \
                                 glob.glob(os.path.join(os.getcwd(), "*_signed.pdf"))
                    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
                for p in candidates:
                    if os.path.exists(p):
                        final_path = p
                        assinou = True
                        break
        except Exception as e:
            print("JSignPdf erro:", e)

    # ----- 3) Overlay do bloco digital (QR + textos + assinatura img) -----
    if assinou:
        try:
            base_url = PUBLIC_BASE_URL if PUBLIC_BASE_URL else request.url_root.rstrip("/")
            url_validacao = f"{base_url}/validar_receita/{receita_id}"

            overlay_buf = io.BytesIO()
            ov = canvas.Canvas(overlay_buf, pagesize=pagesize)

            linha_centro_y = 120
            qr_size = 60
            qr_x = 50
            qr_y = linha_centro_y - qr_size // 2
            text_x = qr_x + qr_size + 14

            ov.drawImage(ImageReader(gerar_qrcode(url_validacao)), qr_x, qr_y, qr_size, qr_size, mask='auto')

            assin_path = _clean_path(assinatura_img_path)
            if assin_path and os.path.exists(assin_path) and assin_path.lower().endswith(('.png', '.jpg', '.jpeg')):
                try:
                    with Image.open(assin_path) as img:
                        if img.mode != "RGBA":
                            img = img.convert("RGBA")
                        bbox = img.getbbox()
                        if bbox:
                            img = img.crop(bbox)
                        iw, ih = img.size
                        max_w = 330 if tamanho_papel == 'A4' else 260
                        max_h = 75 if tamanho_papel == 'A4' else 54
                        ratio = min(max_w / iw, max_h / ih, 1.0)
                        nw, nh = int(iw * ratio), int(ih * ratio)
                        tmp_ass = os.path.join(tempfile.gettempdir(), "assinatura_tmp_receita.png")
                        img.resize((nw, nh), Image.LANCZOS).save(tmp_ass)
                    centro_x = largura / 2.0
                    x_ass = int(centro_x - (nw / 2))
                    y_ass = linha_centro_y + 18
                    ov.drawImage(tmp_ass, x_ass, y_ass, width=nw, height=nh, mask='auto')
                    try:
                        os.remove(tmp_ass)
                    except Exception:
                        pass
                except Exception:
                    pass

            ov.setFont("Helvetica-Bold", 11)
            ov.drawString(text_x, linha_centro_y + 15, f"{nome_medico}    |    {conselho_label}")
            ov.setFont("Helvetica", 9)
            ov.drawString(text_x, linha_centro_y, "Para verificar a autenticidade da receita, leia o QR code ao lado.")
            ov.drawString(text_x, linha_centro_y - 15, "Assinatura digital válida conforme MP 2.200-2/2001")
            ov.save()
            overlay_buf.seek(0)

            reader = PdfReader(final_path)
            over_reader = PdfReader(overlay_buf)
            over_page = over_reader.pages[0]

            writer = PdfWriter()
            for pg in reader.pages:
                page = pg
                if Transformation and hasattr(page, "merge_transformed_page"):
                    page.merge_transformed_page(over_page, Transformation().scale(1, 1))
                else:
                    page.merge_page(over_page)
                writer.add_page(page)

            with open(final_path, "wb") as f:
                writer.write(f)
        except Exception as e:
            print("Overlay bloco digital falhou:", e)

    # ----- 4) Grava definitivo, atualiza banco e responde -----
    with open(final_path, 'rb') as f:
        pdf_bytes_final = f.read()
    with open(caminho_arquivo, 'wb') as dst:
        dst.write(pdf_bytes_final or b"")

    try:
        if os.path.exists(base_path) and base_path != final_path:
            os.remove(base_path)
    except Exception:
        pass

    try:
        conn = _db()
        cur = conn.cursor()
        cur.execute("UPDATE receitas SET pdf_assinado_path=%s WHERE id=%s",
                    (caminho_arquivo.replace('\\', '/'), receita_id))
        cur.close()
        conn.close()
    except Exception as e:
        print("UPDATE caminho PDF falhou:", e)

    nome_download = 'receita_assinada.pdf' if assinou else 'receita.pdf'
    return send_file(
        io.BytesIO(pdf_bytes_final or b""),
        mimetype='application/pdf',
        as_attachment=True,
        download_name=nome_download
    )

# -------------------- arquivos e validação --------------------
@receita_bp.route('/receitas/<nome_arquivo>')
def servir_receita(nome_arquivo):
    return send_from_directory(PASTA_RECEITAS, nome_arquivo)

@receita_bp.route('/validar_receita/<int:receita_id>')
def validar_receita(receita_id):
    try:
        conn = _db()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                r.texto, r.data_emissao, r.status, r.pdf_assinado_path,
                m.id, m.nome, m.crm, COALESCE(m.assinatura_img_path,''),
                p.nome, p.cpf
            FROM receitas r
            JOIN medicos m ON r.medico_id = m.id
            JOIN pacientes p ON r.paciente_id = p.id
            WHERE r.id=%s
        """, (receita_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return "Receita não encontrada", 404

        texto, data_emissao, status, pdf_path, med_id, nome_med, crm, ass_path, nome_pac, cpf_pac = row
        conselho_label = montar_conselho_label(int(med_id), crm)

        assinatura_data_uri = None
        ass_path = _clean_path(ass_path)
        if ass_path and os.path.exists(ass_path):
            mime = "image/png" if ass_path.lower().endswith(".png") else "image/jpeg"
            with open(ass_path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode("ascii")
            assinatura_data_uri = f"data:{mime};base64,{b64}"

        pdf_nome = ""
        if pdf_path:
            pdf_nome = str(pdf_path).strip().replace("\r", "").replace("\n", "").split("/")[-1].split("\\")[-1]

        return render_template(
            "validar_receita.html",
            status=status,
            nome_medico=nome_med,
            conselho_label=conselho_label,
            assinatura_data_uri=assinatura_data_uri,
            nome_paciente=nome_pac,
            cpf_paciente=fmt_cpf(cpf_pac),
            data_emissao=(data_emissao.strftime("%d/%m/%Y") if isinstance(data_emissao, datetime) else str(data_emissao)),
            texto=texto or "",
            pdf_filename=pdf_nome
        )
    except Exception as e:
        return f"Erro interno: {e}", 500

# -------------------- paciente helper --------------------
def _get_or_create_paciente(conn, nome, cpf, data_nascimento, sexo):
    cur = conn.cursor()
    cur.execute("SELECT id FROM pacientes WHERE cpf=%s", (cpf,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO pacientes (nome, cpf, data_nascimento, sexo, criado_em) "
        "VALUES (%s,%s,%s,%s,%s)",
        (nome, cpf, data_nascimento, sexo, datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S'))
    )
    conn.commit()
    return cur.lastrowid
