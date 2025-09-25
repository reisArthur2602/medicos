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

load_dotenv()

TZ = pytz.timezone('America/Sao_Paulo')
os.environ['TZ'] = 'America/Sao_Paulo'

CAMINHO_JSIGNPDF = os.getenv("JSIGNPDF_JAR", r"JSignPdf.jar")
PUBLIC_BASE_URL  = (os.getenv("PUBLIC_BASE_URL") or os.getenv("NGROK_URL") or "").rstrip("/")
PASTA_DECLARACOES = os.path.join(os.getcwd(), "declaracoes")
os.makedirs(PASTA_DECLARACOES, exist_ok=True)

declaracao_bp = Blueprint('declaracao', __name__)

# -------------------- helpers --------------------
def _db_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        connect_timeout=15,
        charset="utf8mb4",
        autocommit=True
    )

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
        y, m, d = s[:4], s[5:7], s[8:10]
        return f"{d}/{m}/{y}"
    if len(s) == 8 and s.isdigit():
        y, m, d = s[:4], s[4:6], s[6:8]
        return f"{d}/{m}/{y}"
    return s

def norm_date_sql(d):
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

def desenhar_texto_multilinha(pdf, texto, x, y, largura_caixa,
                              fontname="Helvetica", fontsize=11, leading=14):
    from reportlab.pdfbase.pdfmetrics import stringWidth
    for linha_usuario in str(texto or '').replace('\r\n', '\n').replace('\r', '\n').split('\n'):
        palavras = linha_usuario.split(' ')
        linha_atual = ""
        for palavra in palavras:
            test = (linha_atual + " " if linha_atual else "") + palavra
            if stringWidth(test, fontname, fontsize) < largura_caixa:
                linha_atual = test
            else:
                if linha_atual:
                    pdf.drawString(x, y, linha_atual)
                    y -= leading
                linha_atual = palavra
        if linha_atual:
            pdf.drawString(x, y, linha_atual)
            y -= leading
    return y

def get_or_create_paciente(conn, nome, cpf, data_nascimento, sexo):
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

def obter_dados_medico(medico_id: int):
    """Retorna: certificado_path, certificado_senha, assinatura_img_path, crm, nome"""
    try:
        conn = _db_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COALESCE(certificado_path,''),
                COALESCE(certificado_senha,''),
                COALESCE(assinatura_img_path,''),
                COALESCE(crm,''),
                COALESCE(nome,'')
            FROM medicos WHERE id=%s
        """, (medico_id,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return (None, None, None, '', '')
        cert, senha, assin, crm, nome = map(_clean, row)
        return (_clean_path(cert), (senha or '').strip(), _clean_path(assin), crm or '', nome or '')
    except Exception as e:
        print("Erro obter_dados_medico:", e)
        return (None, None, None, '', '')

# ---------- NOVO: monta rótulo do conselho a partir da tabela `conselho`
def montar_conselho_label(medico_id: int, crm_fallback: str = "") -> str:
    """
    Busca em `conselho` (tipo, codigo, uf) pelo medico_id (último registro).
    Formata como 'CRO-RJ 0000' / 'CRM-SP 123' etc.
    Se não achar, usa fallback: 'CRM {crm_fallback}' (se houver) ou 'Registro profissional'.
    """
    try:
        conn = _db_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT tipo, codigo, uf
              FROM conselho
             WHERE medico_id=%s
          ORDER BY id DESC
             LIMIT 1
        """, (medico_id,))
        row = cur.fetchone()
        cur.close(); conn.close()
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

# ----------- NOVA LÓGICA: papel via tabelas novas -----------
def _obter_cfg_papel(medico_id: int, doc_tipo: str):
    padrao = None
    a4_path, a5_path = None, None
    try:
        conn = _db_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT tamanho, caminho
                  FROM papeis_timbrados
                 WHERE medico_id=%s AND ativo=1
            """, (medico_id,))
            for t, c in cur.fetchall():
                t = (t or "").upper()
                c = _clean_path(c)
                if t == "A4": a4_path = c
                elif t == "A5": a5_path = c
        except Exception:
            pass
        try:
            cur.execute("""
                SELECT tamanho_padrao
                  FROM preferencias_papel_medico
                 WHERE medico_id=%s AND doc_tipo=%s
                 LIMIT 1
            """, (medico_id, doc_tipo.upper()))
            row = cur.fetchone()
            if row and row[0] in ("A4", "A5"):
                padrao = row[0]
        except Exception:
            pass
        cur.close(); conn.close()
    except Exception:
        pass

    if padrao not in ("A4", "A5"):
        env_key = f"DEFAULT_PAPER_{doc_tipo.upper()}"
        padrao = (os.getenv(env_key) or "A4").strip().upper()
        if padrao not in ("A4", "A5"):
            padrao = "A4"
    return {"padrao": padrao, "a4_path": a4_path, "a5_path": a5_path}

# -------------------- endpoints --------------------
@declaracao_bp.route('/api/gerar-declaracao', methods=['POST'])
def gerar_declaracao():
    data = request.get_json(force=True, silent=True) or {}
    medico_id = int(data.get('medico_id') or 0)
    if not medico_id:
        return {'erro': 'medico_id é obrigatório'}, 400

    cert_path, cert_senha, assinatura_img_path, crm_medico, nome_medico = obter_dados_medico(medico_id)
    conselho_label = montar_conselho_label(medico_id, crm_medico)   # <<---- AQUI

    # papel via backend (novas tabelas)
    cfg = _obter_cfg_papel(medico_id, doc_tipo="DECLARACAO")
    tamanho_papel = cfg["padrao"]
    if tamanho_papel == "A5":
        pagesize = A5; papel_timbrado_path = cfg["a5_path"]
    else:
        pagesize = A4; papel_timbrado_path = cfg["a4_path"]

    largura, altura = pagesize

    # dados do paciente/texto
    nome_paciente = _clean(data.get('nome_paciente') or 'Paciente')
    cpf_paciente  = fmt_cpf(data.get('cpf_paciente') or '')
    data_nasc_in  = _clean(data.get('data_nascimento') or '')
    nasc_fmt      = fmt_data(data_nasc_in) if data_nasc_in else ''
    nasc_sql      = norm_date_sql(data_nasc_in) if data_nasc_in else None

    data_decl_str = fmt_data(_clean(data.get('data_declaracao') or datetime.now(TZ).strftime('%Y-%m-%d')))
    hora_inicio   = _clean(data.get('hora_inicio') or '')
    hora_fim      = _clean(data.get('hora_fim') or '')
    data_emissao_dt = datetime.now(TZ)
    data_emissao    = data_emissao_dt.strftime('%d/%m/%Y')

    # criar/obter paciente
    try:
        conn = _db_conn()
        paciente_id = get_or_create_paciente(conn, nome_paciente, cpf_paciente, nasc_sql, None)
        cur = conn.cursor()
        texto_linhas = [
            f"Declaro, para os devidos fins, que {nome_paciente},",
            f"portador(a) do CPF nº {cpf_paciente}, compareceu à consulta médica no dia {data_decl_str}, das {hora_inicio} às {hora_fim}."
        ]
        cur.execute("""
            INSERT INTO declaracoes (medico_id, paciente_id, texto, data_emissao,
                                     pdf_assinado_path, assinado_em, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            medico_id, int(paciente_id), '\n'.join(texto_linhas),
            data_emissao_dt.strftime('%Y-%m-%d'),
            "TEMP", data_emissao_dt.strftime('%Y-%m-%d %H:%M:%S'), 1
        ))
        declaracao_id = cur.lastrowid
        cur.close(); conn.close()
    except Exception as e:
        return {'erro': f'Falha ao salvar no banco: {e}'}, 500

    has_cert = bool(cert_path and cert_senha and os.path.isfile(cert_path))

    # gerar PDF
    import uuid
    nome_arquivo = f"declaracao_{uuid.uuid4()}.pdf"
    caminho_arquivo = os.path.join(PASTA_DECLARACOES, nome_arquivo)

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=pagesize)

    # fundo (se existir)
    try:
        if papel_timbrado_path and os.path.exists(papel_timbrado_path):
            desenhar_fundo_papel(pdf, papel_timbrado_path, largura, altura)
    except Exception as e:
        print("Falha ao aplicar fundo:", e)

    # cabeçalho
    margem_x = 25 if tamanho_papel == 'A5' else 50
    largura_texto = largura - (2 * margem_x)
    y_inicial = altura - 70

    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(margem_x, y_inicial, "DECLARAÇÃO MÉDICA")

    pdf.setFont("Helvetica", 11)
    y = y_inicial - 25
    pdf.drawString(margem_x, y, f"Paciente: {nome_paciente}")
    y -= 18
    if cpf_paciente:
        pdf.drawString(margem_x, y, f"CPF: {cpf_paciente}")
        y -= 18
    if nasc_fmt:
        pdf.drawString(margem_x, y, f"Nascimento: {nasc_fmt}")
        y -= 18

    # bloco de texto
    texto_linhas = [
        f"Declaro, para os devidos fins, que {nome_paciente},",
        f"portador(a) do CPF nº {cpf_paciente}, compareceu à consulta médica no dia {data_decl_str}, das {hora_inicio} às {hora_fim}."
    ]
    y_assin_top = 180
    bloco_texto_linhas = len(texto_linhas)
    bloco_altura = bloco_texto_linhas * 17
    y_min = y_assin_top + bloco_altura
    y_texto = (y - 18) - ((y - 18 - y_min) // 2)

    pdf.setFont("Helvetica", 11)
    for t in texto_linhas:
        y_texto = desenhar_texto_multilinha(pdf, t, margem_x, y_texto, largura_texto, fontsize=11, leading=15)
        y_texto -= 2

    pdf.setFont("Helvetica", 10)
    y_texto -= 10
    pdf.drawString(margem_x, y_texto, f"Data de emissão: {data_emissao}")

    # rodapé
    if has_cert:
        base = PUBLIC_BASE_URL if PUBLIC_BASE_URL else request.url_root.rstrip('/')
        url_validacao = f"{base}/validar_declaracao/{declaracao_id}"

        qr_size = 60
        margem_esquerda = 50
        linha_altura = 15
        linha_centro_y = 135
        qr_y = linha_centro_y - qr_size // 2
        texto_x = margem_esquerda + qr_size + 14
        centro_x = largura // 2

        qr_buffer = gerar_qrcode(url_validacao)
        pdf.drawImage(ImageReader(qr_buffer), margem_esquerda, qr_y, qr_size, qr_size, mask='auto')

        assinatura_img_path = _clean_path(assinatura_img_path)
        if assinatura_img_path and os.path.exists(assinatura_img_path) and assinatura_img_path.lower().endswith(('.png', '.jpg', '.jpeg')):
            try:
                with Image.open(assinatura_img_path) as img:
                    if img.mode != "RGBA":
                        img = img.convert("RGBA")
                    bbox = img.getbbox()
                    if bbox:
                        img = img.crop(bbox)
                    img_w, img_h = img.size
                    max_w = 330 if tamanho_papel == 'A4' else 260
                    max_h = 75  if tamanho_papel == 'A4' else 54
                    ratio = min(max_w / img_w, max_h / img_h, 1.0)
                    new_w = int(img_w * ratio); new_h = int(img_h * ratio)
                    temp_assin = os.path.join(tempfile.gettempdir(), "assinatura_tmp_decl.png")
                    img.resize((new_w, new_h), Image.LANCZOS).save(temp_assin)
                x_ass = centro_x - (new_w // 2)
                y_ass = linha_centro_y + linha_altura + 3
                pdf.drawImage(temp_assin, x_ass, y_ass, width=new_w, height=new_h, mask='auto')
                try: os.remove(temp_assin)
                except: pass
            except Exception as e:
                print("Falha preparar assinatura imagem:", e)

        pdf.setFont("Helvetica-Bold", 11)
        pdf.drawString(texto_x, linha_centro_y + linha_altura, f"{nome_medico}    |    {conselho_label}")  # <<--- sem 'CRM:'
        pdf.setFont("Helvetica", 9)
        pdf.drawString(texto_x, linha_centro_y, "Para verificar a autenticidade da declaração, leia o QR code ao lado.")
        pdf.drawString(texto_x, linha_centro_y - linha_altura, "Assinatura digital válida conforme MP 2.200-2/2001")
    else:
        pdf.setLineWidth(1)
        cx = largura / 2.0
        linha_w = 320 if tamanho_papel == 'A4' else 260
        y_linha = 100
        pdf.line(cx - linha_w/2, y_linha, cx + linha_w/2, y_linha)
        pdf.setFont("Helvetica", 10)
        pdf.drawCentredString(cx, y_linha - 12, "Assinatura e carimbo do médico")

    pdf.save()
    buffer.seek(0)

    # assinar digitalmente (se houver certificado)
    pdf_bytes_final = buffer.getvalue()
    if has_cert:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_in:
            tmp_in.write(pdf_bytes_final)
            tmp_in_path = tmp_in.name

        cmd = [
            'java', '-jar', CAMINHO_JSIGNPDF,
            '-kst', 'PKCS12',
            '-ksf', cert_path,
            '-ksp', cert_senha,
            tmp_in_path
        ]
        tmp_signed_path = None
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode == 0:
                base = os.path.splitext(os.path.basename(tmp_in_path))[0]
                candidates = [
                    os.path.join(os.path.dirname(tmp_in_path), base + "_signed.pdf"),
                    os.path.join(os.getcwd(), base + "_signed.pdf"),
                ]
                if not any(os.path.exists(p) for p in candidates):
                    candidates = glob.glob(os.path.join(os.path.dirname(tmp_in_path), "*_signed.pdf")) + \
                                 glob.glob(os.path.join(os.getcwd(), "*_signed.pdf"))
                    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
                for p in candidates:
                    if os.path.exists(p):
                        tmp_signed_path = p
                        break
                if tmp_signed_path:
                    with open(tmp_signed_path, 'rb') as f:
                        pdf_bytes_final = f.read()
            else:
                print("JSignPdf falhou:", proc.stderr)
        finally:
            try:
                if os.path.exists(tmp_in_path): os.remove(tmp_in_path)
            except: pass
            try:
                if tmp_signed_path and os.path.exists(tmp_signed_path): os.remove(tmp_signed_path)
            except: pass

    # salvar arquivo final
    with open(caminho_arquivo, 'wb') as f:
        f.write(pdf_bytes_final)

    # atualizar caminho no banco
    try:
        conn = _db_conn()
        cur = conn.cursor()
        cur.execute("UPDATE declaracoes SET pdf_assinado_path=%s WHERE id=%s",
                    (caminho_arquivo.replace('\\','/'), declaracao_id))
        cur.close(); conn.close()
    except Exception as e:
        print("UPDATE caminho PDF falhou:", e)

    # resposta
    base_public = PUBLIC_BASE_URL if PUBLIC_BASE_URL else request.url_root.rstrip('/')
    return {
        "status": "ok",
        "url": f"{base_public}/declaracoes/{os.path.basename(caminho_arquivo)}",
        "validar": f"{base_public}/validar_declaracao/{declaracao_id}" if has_cert else None,
        "caminho": caminho_arquivo.replace('\\','/')
    }, 200

@declaracao_bp.route('/declaracoes/<nome_arquivo>')
def servir_declaracao(nome_arquivo):
    return send_from_directory(PASTA_DECLARACOES, nome_arquivo)

@declaracao_bp.route('/validar_declaracao/<int:declaracao_id>')
def validar_declaracao(declaracao_id):
    try:
        conn = _db_conn()
        cur = conn.cursor()
        # pega também o id do médico para montar o label
        cur.execute("""
            SELECT
                d.texto, d.data_emissao, d.status, d.pdf_assinado_path,
                m.id, m.nome, m.crm, COALESCE(m.assinatura_img_path,''),
                p.nome, p.cpf
            FROM declaracoes d
            JOIN medicos m ON d.medico_id = m.id
            JOIN pacientes p ON d.paciente_id = p.id
            WHERE d.id=%s
        """, (declaracao_id,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return "Declaração não encontrada", 404

        texto, data_emissao, status, pdf_path, med_id, nome_med, crm, ass_path, nome_pac, cpf_pac = row
        conselho_label = montar_conselho_label(int(med_id), crm)  # <<--- usa tabela conselho

        assinatura_data_uri = None
        ass_path = _clean_path(ass_path)
        if ass_path and os.path.exists(ass_path):
            mime = "image/png" if ass_path.lower().endswith(".png") else "image/jpeg"
            with open(ass_path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode("ascii")
            assinatura_data_uri = f"data:{mime};base64,{b64}"

        pdf_nome = ""
        if pdf_path:
            pdf_nome = str(pdf_path).strip().replace("\r","").replace("\n","").split("/")[-1].split("\\")[-1]

        return render_template(
            "validar_declaracao.html",
            status=status,
            nome_medico=nome_med,
            conselho_label=conselho_label,   # <<--- passa label pronto
            assinatura_data_uri=assinatura_data_uri,
            nome_paciente=nome_pac,
            cpf_paciente=fmt_cpf(cpf_pac),
            data_emissao=(data_emissao.strftime("%d/%m/%Y") if isinstance(data_emissao, datetime) else fmt_data(data_emissao)),
            texto=texto or "",
            pdf_filename=pdf_nome
        )
    except Exception as e:
        return f"Erro interno: {e}", 500
