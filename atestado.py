# atestado.py
from flask import Blueprint, request, send_file, send_from_directory, render_template, abort, url_for, current_app
import os, io, tempfile, subprocess, glob, shutil
import pymysql, pytz
from reportlab.lib.pagesizes import A4, A5
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from PIL import Image
from datetime import datetime, timedelta
from dotenv import load_dotenv

from PyPDF2 import PdfReader, PdfWriter
try:
    from PyPDF2 import Transformation
except Exception:
    Transformation = None

# utils do projeto (mantidos)
from utils import (
    gerar_qrcode,
    desenhar_fundo_papel,
    desenhar_texto_multilinha,
    get_or_create_paciente,
)

load_dotenv()
atestado_bp = Blueprint('atestado', __name__)

TZ = pytz.timezone('America/Sao_Paulo')
PUBLIC_BASE_URL = (os.getenv("PUBLIC_BASE_URL") or os.getenv("NGROK_URL") or "").rstrip("/")
PASTA_ATESTADOS = os.path.join(os.getcwd(), "atestados")
os.makedirs(PASTA_ATESTADOS, exist_ok=True)
CAMINHO_JSIGNPDF = os.getenv("JSIGNPDF_JAR", "JSignPdf.jar")


# ---------------- helpers ----------------
def _db_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        connect_timeout=15,
        charset="utf8mb4",
        autocommit=True,
    )

def _nome_limpinho(nome: str) -> str:
    if not nome:
        return ""
    s = " ".join(str(nome).replace("\x00", "").split())
    if s.endswith(","):
        s = s[:-1].rstrip()
    return s

def fmt_data(d):
    if not d:
        return ""
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

def fmt_cpf(cpf):
    s = ''.join(ch for ch in str(cpf or '') if ch.isdigit())
    if len(s) == 11:
        return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}"
    return str(cpf or '')

def _clean(s):
    if s is None: return None
    return str(s).strip().replace('\x00','')

def _clean_path(p):
    if not p: return None
    return str(p).strip().strip('"').replace('\r','').replace('\n','')

# === Pega SOMENTE colunas existentes em `medicos`
def obter_dados_medico_basico(medico_id: int):
    """
    Retorna: (cert_path, cert_senha, assinatura_img_path, crm_texto, nome)
    """
    try:
        conn = _db_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT
              COALESCE(certificado_path,''),
              COALESCE(certificado_senha,''),
              COALESCE(assinatura_img_path,''),
              COALESCE(crm,''),
              COALESCE(nome,'')
            FROM medicos
            WHERE id=%s
        """, (medico_id,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row:
            return (None, None, None, '', '')
        cert, senha, assin, crm, nome = map(_clean, row)
        return (_clean_path(cert), (senha or '').strip(), _clean_path(assin), crm or '', nome or '')
    except Exception as e:
        current_app.logger.exception("Erro ao consultar médico: %s", e)
        return (None, None, None, '', '')

# === Busca rótulo do conselho (tabela nova), com fallback para medicos.crm
def obter_conselho_rotulo(medico_id: int, crm_fallback: str = "") -> tuple[str, str]:
    """
    Retorna (rotulo, tipo_upper)
      - Ex.: ("CRO: 0000-RJ", "CRO")
    Fallback: "CRM: <crm_fallback>" se não houver registro em `conselho`.
    """
    tipo, codigo, uf = None, None, None
    try:
        conn = _db_conn(); cur = conn.cursor()
        # Pega o último conselho cadastrado para o médico
        cur.execute("""
            SELECT tipo, codigo, COALESCE(uf,'')
            FROM conselho
            WHERE medico_id=%s
            ORDER BY id DESC
            LIMIT 1
        """, (medico_id,))
        r = cur.fetchone()
        cur.close(); conn.close()
        if r:
            tipo = (r[0] or '').strip().upper()
            codigo = (r[1] or '').strip()
            uf = (r[2] or '').strip().upper()
    except Exception as e:
        current_app.logger.warning("Falha lendo conselho: %s", e)

    if tipo and codigo:
        rot = f"{tipo}: {codigo}{('-' + uf) if uf else ''}"
        return rot, tipo
    # fallback em `medicos.crm`
    crm_fallback = (crm_fallback or '').strip()
    if crm_fallback:
        return f"CRM: {crm_fallback}", "CRM"
    return "CRM: ", "CRM"  # último fallback

# ----------- papel pelas TABELAS NOVAS (com fallback .env) ----------- #
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
                t = (t or "").strip().upper()
                c = (c or "").strip().strip('"').replace("\r", "").replace("\n", "")
                if t == "A4": a4_path = c
                elif t == "A5": a5_path = c
        except Exception as e:
            current_app.logger.warning("[ATESTADO] Falha lendo papeis_timbrados: %s", e)

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
        except Exception as e:
            current_app.logger.warning("[ATESTADO] Falha lendo preferencias_papel_medico: %s", e)

        cur.close(); conn.close()
    except Exception as e:
        current_app.logger.warning("[ATESTADO] _obter_cfg_papel erro conexão: %s", e)

    if padrao not in ("A4", "A5"):
        env_key = f"DEFAULT_PAPER_{doc_tipo.upper()}"
        padrao = (os.getenv(env_key) or "A4").strip().upper()
        if padrao not in ("A4", "A5"):
            padrao = "A4"

    return {"padrao": padrao, "a4_path": a4_path, "a5_path": a5_path}


# ---------------- rotas de arquivos ----------------
@atestado_bp.route('/assinatura_medico/<nome_arquivo>')
def assinatura_medico(nome_arquivo):
    pasta_assinaturas = os.getenv("ASSINATURAS_DIR", r"C:\Users\T.I\Desktop\medicos\assinaturas")
    caminho = os.path.join(pasta_assinaturas, nome_arquivo)
    if os.path.exists(caminho):
        return send_file(caminho)
    return abort(404)

@atestado_bp.route('/atestados/<nome_arquivo>')
def servir_atestado(nome_arquivo):
    return send_from_directory(PASTA_ATESTADOS, nome_arquivo)


# ---------------- validação pública ----------------
@atestado_bp.route('/validar_atestado/<int:atestado_id>')
def validar_atestado(atestado_id):
    conn = _db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            a.texto, a.data_emissao, a.dias_afastamento, a.status,
            m.id, m.nome, m.crm, COALESCE(m.assinatura_img_path, ''),
            p.nome, p.cpf, COALESCE(a.pdf_assinado_path, '')
        FROM atestados a
        JOIN medicos m ON a.medico_id = m.id
        JOIN pacientes p ON a.paciente_id = p.id
        WHERE a.id = %s
    """, (atestado_id,))
    r = cur.fetchone()
    cur.close(); conn.close()
    if not r:
        return "Atestado não encontrado", 404

    (texto, data_emissao, dias_afastamento, status,
     mid, nome_medico, crm_txt, assinatura_img_path,
     nome_paciente, cpf_paciente, pdf_assinado_path) = r

    rotulo_conselho, tipo_conselho = obter_conselho_rotulo(int(mid), crm_fallback=crm_txt)

    assinatura_url = None
    assinatura_img_path = (assinatura_img_path or "").strip()
    if assinatura_img_path:
        assinatura_nome = assinatura_img_path.replace("\\", "/").split("/")[-1]
        try:
            assinatura_url = url_for('atestado.assinatura_medico', nome_arquivo=assinatura_nome, _external=True)
        except Exception:
            assinatura_url = None

    pdf_nome = os.path.basename(str(pdf_assinado_path).strip()) if pdf_assinado_path else ""

    return render_template(
        "validar_atestado.html",
        status=status,
        nome_medico=nome_medico,
        crm=rotulo_conselho,                 # compat: se o template mostra "CRM: {{ crm }}", já vem completo
        conselho_label=rotulo_conselho,      # novo (se quiser usar no HTML)
        conselho_tipo=tipo_conselho,         # novo
        assinatura_url=assinatura_url,
        nome_paciente=nome_paciente,
        cpf_paciente=fmt_cpf(cpf_paciente),
        data_emissao=data_emissao.strftime("%d/%m/%Y") if isinstance(data_emissao, datetime) else fmt_data(data_emissao),
        texto=texto,
        dias_afastamento=dias_afastamento,
        pdf_nome=pdf_nome
    )


# ---------------- geração do atestado ----------------
@atestado_bp.route('/api/gerar-atestado', methods=['POST'])
def gerar_atestado():
    """
    - NÃO usa tamanho de papel do JSON.
    - Papel: tabelas novas (papeis_timbrados + preferencias_papel_medico) e fallback .env.
    - Bloco digital (QR + textos) só aparece se assinar digitalmente.
    """
    data = request.get_json(silent=True) or {}
    medico_id = data.get('medico_id')
    if not medico_id:
        return {'erro': 'medico_id é obrigatório'}, 400
    medico_id = int(medico_id)

    cert_path, cert_pass, assinatura_img_path, crm_txt, nome_medico = obter_dados_medico_basico(medico_id)
    conselho_label, conselho_tipo = obter_conselho_rotulo(medico_id, crm_fallback=crm_txt)

    papel_cfg = _obter_cfg_papel(medico_id, doc_tipo="ATESTADO")
    tamanho_papel = papel_cfg["padrao"]
    if tamanho_papel == "A5":
        pagesize = A5
        papel_timbrado_path = papel_cfg["a5_path"]
    else:
        pagesize = A4
        papel_timbrado_path = papel_cfg["a4_path"]

    largura, altura = pagesize
    current_app.logger.info("[ATESTADO] Papel=%s | A4=%s | A5=%s | usando=%s",
                            tamanho_papel, papel_cfg["a4_path"], papel_cfg["a5_path"], papel_timbrado_path)

    # ---- Dados do conteúdo
    nome_paciente    = _nome_limpinho(data.get('nome_paciente', 'Paciente'))
    cpf_paciente     = data.get('cpf_paciente', '')
    data_nascimento  = data.get('data_nascimento', None)
    nasc_fmt         = fmt_data(data_nascimento) if data_nascimento else ""
    sexo             = data.get('sexo', None)
    cid              = data.get('cid', 'CID-XXX')
    dias_afastamento = int(data.get('dias_afastamento', 1))

    data_emissao_dt = datetime.now(TZ)
    data_emissao    = data_emissao_dt.strftime('%d/%m/%Y')
    data_fim_dt     = data_emissao_dt + timedelta(days=dias_afastamento - 1)
    data_fim        = data_fim_dt.strftime('%d/%m/%Y')

    assinatura_img_path = (assinatura_img_path or "").strip()

    # Logs de pré-checagem
    current_app.logger.info(
        "[ATESTADO] java_ok=%s jsignpdf_exists=%s cert=%s cert_exists=%s pass=%s",
        shutil.which("java") is not None,
        os.path.isfile(CAMINHO_JSIGNPDF),
        cert_path,
        os.path.isfile(cert_path or ""),
        bool((cert_pass or "").strip())
    )

    # 1) Salva no banco e obtém ID
    conn = _db_conn()
    paciente_id = get_or_create_paciente(conn, nome_paciente, cpf_paciente, data_nascimento, sexo)
    cur = conn.cursor()
    texto_atestado = "\n".join([
        f"Atesto, para os devidos fins, que {nome_paciente}, portador(a) do CPF nº {fmt_cpf(cpf_paciente)},",
        f"foi submetido(a) a consulta médica na data de {data_emissao}.",
        f"Diagnóstico (CID): {cid}.",
        f"Deverá permanecer afastado(a) de suas atividades laborativas por {dias_afastamento} dia(s),",
        "a partir desta data.",
        f"Atestado válido de {data_emissao} até {data_fim}."
    ])
    cur.execute("""
        INSERT INTO atestados 
        (medico_id, paciente_id, texto, dias_afastamento, data_emissao, pdf_assinado_path, assinado_em, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        medico_id, paciente_id, texto_atestado, dias_afastamento, data_emissao_dt.strftime('%Y-%m-%d'),
        "TEMP", data_emissao_dt.strftime('%Y-%m-%d %H:%M:%S'), 1
    ))
    atestado_id = cur.lastrowid
    conn.commit()

    # 2) Gera PDF base (sem bloco digital)
    import uuid
    nome_arquivo = f"atestado_{uuid.uuid4()}.pdf"
    caminho_arquivo = os.path.join(PASTA_ATESTADOS, nome_arquivo)

    margem_x = 25 if tamanho_papel == 'A5' else 50
    largura_texto = largura - (2 * margem_x)
    y_inicial = altura - 70

    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_pdf:
        buffer = io.BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=pagesize)

        # fundo (se houver)
        try:
            if papel_timbrado_path and os.path.exists(papel_timbrado_path):
                desenhar_fundo_papel(pdf, papel_timbrado_path, largura, altura)
        except Exception as e:
            current_app.logger.warning("[ATESTADO] Falha ao aplicar fundo: %s", e)

        # Cabeçalho
        pdf.setFont("Helvetica-Bold", 14)
        pdf.drawString(margem_x, y_inicial, "ATESTADO MÉDICO")

        pdf.setFont("Helvetica", 11)
        y = y_inicial - 25
        pdf.drawString(margem_x, y, f"Paciente: {nome_paciente}")
        y -= 18
        if cpf_paciente:
            pdf.drawString(margem_x, y, f"CPF: {fmt_cpf(cpf_paciente)}")
            y -= 18
        if nasc_fmt:
            pdf.drawString(margem_x, y, f"Nascimento: {nasc_fmt}")
            y -= 18
        pdf.drawString(margem_x, y, f"CID: {cid}")

        # Corpo (centralizado verticalmente acima da área de assinatura)
        y_assin_top = 180
        bloco_texto_linhas = 6
        bloco_altura = bloco_texto_linhas * 17
        y_min = y_assin_top + bloco_altura
        y_texto = y - ((y - y_min) // 2)

        pdf.setFont("Helvetica", 11)
        for txt in texto_atestado.splitlines():
            y_texto = desenhar_texto_multilinha(pdf, txt, margem_x, y_texto, largura_texto,
                                                fontname="Helvetica", fontsize=11, leading=15)
            y_texto -= 2

        pdf.setFont("Helvetica", 10)
        y_texto -= 10
        pdf.drawString(margem_x, y_texto, f"Data de emissão: {data_emissao}")

        # Linha para assinatura/carimbo (sempre)
        pdf.setLineWidth(0.8)
        pdf.line(largura*0.25, 85, largura*0.75, 85)
        pdf.setFont("Helvetica", 8.5)
        pdf.drawCentredString(largura*0.5, 72, "Assinatura e carimbo do médico")

        pdf.save()
        buffer.seek(0)
        temp_pdf.write(buffer.getvalue())
        temp_pdf_path = temp_pdf.name

    # 3) Assinatura digital (invisível)
    assinou_digital = False
    final_path = temp_pdf_path

    can_sign = (cert_path and os.path.exists(cert_path) and str(cert_pass or "").strip())
    if can_sign:
        try:
            cmd = [
                'java', '-jar', CAMINHO_JSIGNPDF,
                '-kst', 'PKCS12',
                '-ksf', cert_path,
                '-ksp', str(cert_pass),
                temp_pdf_path
            ]
            current_app.logger.info("[ATESTADO] JSignPdf: %s", " ".join(cmd))
            proc = subprocess.run(cmd, capture_output=True, text=True)
            current_app.logger.info("[ATESTADO] JSignPdf rc=%s", proc.returncode)
            if proc.stdout: current_app.logger.info("[ATESTADO] JSignPdf out: %s", proc.stdout.strip())
            if proc.stderr: current_app.logger.warning("[ATESTADO] JSignPdf err: %s", proc.stderr.strip())

            if proc.returncode == 0:
                base = os.path.splitext(os.path.basename(temp_pdf_path))[0]
                candidates = [
                    os.path.join(os.path.dirname(temp_pdf_path), base + "_signed.pdf"),
                    os.path.join(os.getcwd(), base + "_signed.pdf"),
                ]
                if not any(os.path.exists(p) for p in candidates):
                    candidates = glob.glob(os.path.join(os.path.dirname(temp_pdf_path), "*_signed.pdf")) + \
                                 glob.glob(os.path.join(os.getcwd(), "*_signed.pdf"))
                    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)

                for p in candidates:
                    if os.path.exists(p):
                        final_path = p
                        assinou_digital = True
                        break
        except Exception as e:
            current_app.logger.exception("[ATESTADO] Erro JSignPdf: %s", e)

    # 4) Overlay do bloco digital (QR + assinatura img + rótulo do conselho)
    if assinou_digital:
        try:
            overlay = io.BytesIO()
            ov = canvas.Canvas(overlay, pagesize=pagesize)

            base_url = PUBLIC_BASE_URL if PUBLIC_BASE_URL else request.url_root.rstrip("/")
            link_consulta = f"{base_url}/validar_atestado/{atestado_id}"
            qr_buf = gerar_qrcode(link_consulta)
            qr_size = 60
            ov.drawImage(ImageReader(qr_buf), 50, 135 - 30, qr_size, qr_size, mask='auto')

            # assinatura-imagem (opcional)
            if assinatura_img_path and os.path.exists(assinatura_img_path) and assinatura_img_path.lower().endswith(('.png', '.jpg', '.jpeg')):
                try:
                    max_w = 260 if tamanho_papel == 'A5' else 330
                    max_h = 54  if tamanho_papel == 'A5' else 75
                    with Image.open(assinatura_img_path) as img:
                        if img.mode != "RGBA":
                            img = img.convert("RGBA")
                        bbox = img.getbbox()
                        if bbox:
                            img = img.crop(bbox)
                        iw, ih = img.size
                        ratio = min(max_w/iw, max_h/ih, 1.0)
                        nw, nh = int(iw*ratio), int(ih*ratio)
                        tmp_ass = os.path.join(tempfile.gettempdir(), "assinatura_tmp_atestado.png")
                        img.resize((nw, nh), Image.LANCZOS).save(tmp_ass)
                    centro_x = int(largura//2)
                    x_ass = centro_x - (nw // 2)
                    y_ass = 150
                    ov.drawImage(tmp_ass, x_ass, y_ass, width=nw, height=nh, mask='auto')
                    try: os.remove(tmp_ass)
                    except Exception: pass
                except Exception:
                    pass

            ov.setFont("Helvetica-Bold", 11)
            ov.drawString(50 + qr_size + 14, 150, f"{nome_medico}    |    {conselho_label}")
            ov.setFont("Helvetica", 9)
            ov.drawString(50 + qr_size + 14, 135, "Para verificar a autenticidade do atestado, leia o QR code ao lado.")
            ov.drawString(50 + qr_size + 14, 120, "Assinatura digital válida conforme MP 2.200-2/2001")
            ov.save()
            overlay.seek(0)

            reader = PdfReader(final_path)
            over_reader = PdfReader(overlay)
            page = reader.pages[0]
            over = over_reader.pages[0]

            if Transformation and hasattr(page, "merge_transformed_page"):
                page.merge_transformed_page(over, Transformation().scale(1, 1))
            else:
                page.merge_page(over)

            writer = PdfWriter()
            writer.add_page(page)
            for i in range(1, len(reader.pages)):
                writer.add_page(reader.pages[i])

            with open(final_path, "wb") as f:
                writer.write(f)
        except Exception as e:
            current_app.logger.exception("[ATESTADO] Overlay bloco digital falhou: %s", e)

    # 5) Salva definitivo + atualiza banco
    with open(final_path, 'rb') as f:
        final_bytes = f.read()
    with open(caminho_arquivo, 'wb') as dst:
        dst.write(final_bytes or b"")

    try:
        base_temp = locals().get("temp_pdf_path")
        if base_temp and os.path.exists(base_temp) and base_temp != final_path:
            os.remove(base_temp)
    except Exception:
        pass

    try:
        cur = conn.cursor()
        cur.execute("UPDATE atestados SET pdf_assinado_path=%s WHERE id=%s", (caminho_arquivo, atestado_id))
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        current_app.logger.exception("[ATESTADO] Erro UPDATE caminho PDF: %s", e)

    return send_file(
        io.BytesIO(final_bytes or b""),
        mimetype='application/pdf',
        as_attachment=True,
        download_name='atestado.pdf'
    )
