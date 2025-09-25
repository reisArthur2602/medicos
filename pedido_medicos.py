from flask import Blueprint, request, send_file, jsonify, current_app, render_template
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4, A5
from reportlab.lib.utils import ImageReader
import io, os, tempfile, subprocess, pymysql, qrcode, base64
from PIL import Image
from datetime import datetime
from dotenv import load_dotenv
import pytz
from PyPDF2 import PdfReader, PdfWriter
from werkzeug.utils import secure_filename
from pathlib import Path

# PyPDF2 transform (>=2.10)
try:
    from PyPDF2 import Transformation  # 2.10+ / 3.x
except Exception:
    Transformation = None

load_dotenv()

TZ = pytz.timezone('America/Sao_Paulo')
CAMINHO_JSIGNPDF = os.getenv("JSIGNPDF_JAR", "JSignPdf.jar")
PUBLIC_BASE_URL  = (os.getenv("PUBLIC_BASE_URL") or os.getenv("NGROK_URL") or "").rstrip("/")

PASTA_PEDIDOS    = os.path.join(os.getcwd(), "pedidos_exames")
os.makedirs(PASTA_PEDIDOS, exist_ok=True)

A4_SAFE_RIGHT_MARGIN = int(os.getenv("A4_SAFE_RIGHT_MARGIN", "220"))

exames_bp = Blueprint('exames', __name__)

# -------------------- utils --------------------
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

def _clean(v):
    if v is None:
        return None
    return str(v).strip().replace('\x00', '')

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
        y, m, d2 = s[:4], s[5:7], s[8:10]
        return f"{d2}/{m}/{y}"
    if len(s) == 8 and s.isdigit():
        y, m, d2 = s[:4], s[4:6], s[6:8]
        return f"{d2}/{m}/{y}"
    return s

def gerar_qrcode(url):
    qr = qrcode.QRCode(box_size=3, border=1)
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = io.BytesIO()
    img.save(buffer)
    buffer.seek(0)
    return buffer

# ---------- DADOS DO MÉDICO (apenas tabela medicos; nunca falha por causa de conselho) ----------
def obter_dados_medico(medico_id):
    """
    Retorna dados para assinatura:
      (certificado_path, certificado_senha, assinatura_img_path, crm, nome)
    NÃO faz join com conselhos aqui para não quebrar a assinatura.
    """
    try:
        conn = _db_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                COALESCE(certificado_path,''),
                COALESCE(certificado_senha,''),
                COALESCE(assinatura_img_path,''),
                COALESCE(crm,''),
                COALESCE(nome,'')
            FROM medicos WHERE id = %s
        """, (medico_id,))
        row = cursor.fetchone()
        cursor.close(); conn.close()
        if not row:
            return (None, None, None, "", "")
        cert, senha, ass, crm, nome = map(_clean, row)
        return _clean_path(cert), (senha or "").strip(), _clean_path(ass), (crm or ""), (nome or "")
    except Exception as e:
        current_app.logger.exception("Erro ao obter dados do medico: %s", e)
        return (None, None, None, "", "")

# ---------- RÓTULO DO CONSELHO (igual ao da receita.py) ----------
def montar_conselho_label(medico_id: int, crm_fallback: str = "") -> str:
    """
    Lê a tabela `conselho` por medico_id (colunas: tipo, codigo, uf).
    Retorna 'CRM-SP 12345', 'CRO-RJ 9999', 'CRP 1234' etc.
    Fallback: 'CRM {crm_fallback}' ou 'Registro profissional'.
    """
    try:
        conn = _db_conn(); cur = conn.cursor()
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
                if t == "A4":
                    a4_path = c
                elif t == "A5":
                    a5_path = c
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

def _desenhar_cabecalho(pdf, largura, altura,
                        nome_paciente, cpf_fmt, nasc_fmt,
                        tamanho_papel='A4'):
    margem_x = 50 if tamanho_papel == 'A4' else 25
    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(margem_x, altura - 60, "PEDIDO DE EXAMES")

    pdf.setFont("Helvetica", 11)
    y = altura - 90
    pdf.drawString(margem_x, y, f"Paciente: {nome_paciente}")
    if cpf_fmt:
        y -= 18
        pdf.drawString(margem_x, y, f"CPF: {cpf_fmt}")
    if nasc_fmt:
        y -= 18
        pdf.drawString(margem_x, y, f"Nascimento: {nasc_fmt}")
    y -= 20
    return y

def _desenhar_fundo_imagem(pdf, path_img, largura, altura):
    pdf.drawImage(path_img, 0, 0, width=largura, height=altura)

def _apply_transform(page, transf):
    try:
        page.add_transformation(transf)  # 3.x
    except Exception:
        try:
            page.addTransformation(transf)  # 2.x
        except Exception:
            pass

def _merge_with_bg_as_base(content_path, bg_pdf_path, out_path):
    content_reader = PdfReader(content_path)
    writer = PdfWriter()
    for content_page in content_reader.pages:
        cw = float(content_page.mediabox.width)
        ch = float(content_page.mediabox.height)
        bg_reader = PdfReader(bg_pdf_path)
        bg_page = bg_reader.pages[0]
        try:
            rotate = int(bg_page.get("/Rotate", 0)) or 0
        except Exception:
            rotate = 0
        if rotate % 360 != 0:
            try:
                bg_page = bg_page.rotate(360 - (rotate % 360))
            except Exception:
                try:
                    bg_page.rotateClockwise(360 - (rotate % 360))
                except Exception:
                    pass
        bw = float(bg_page.mediabox.width); bh = float(bg_page.mediabox.height)
        try:
            if Transformation:
                sx, sy = cw / bw, ch / bh
                _apply_transform(bg_page, Transformation().scale(sx, sy))
                bg_page.mediabox.upper_right = (cw, ch)
        except Exception as e:
            current_app.logger.warning("Falha ao escalar timbrado: %s", e)
            try:
                bg_page.mediabox.upper_right = (cw, ch)
            except Exception:
                pass
        try:
            if hasattr(bg_page, "merge_transformed_page") and Transformation:
                bg_page.merge_transformed_page(content_page, Transformation())
            elif hasattr(bg_page, "merge_page"):
                bg_page.merge_page(content_page)
            else:
                bg_page.mergePage(content_page)
        except Exception as e:
            current_app.logger.warning("Falha ao sobrepor conteúdo: %s", e)
        writer.add_page(bg_page)
    with open(out_path, "wb") as f:
        writer.write(f)

def _calc_sig_rect_below_qr(pagesize_name):
    if pagesize_name == 'A5':
        w, h = 260, 60; llx = 50; lly = 40
    else:
        w, h = 300, 70; llx = 50; lly = 40
    urx = llx + w; ury = lly + h
    return llx, lly, urx, ury

def assinar_pdf_jsignpdf(caminho_pdf_entrada: str,
                         caminho_certificado: str,
                         senha_certificado: str,
                         assinatura_img_path: str | None,
                         assinatura_visivel: bool = False,
                         coords_assin=(410, 60, 560, 130),
                         pagina=1) -> str:
    workdir = Path(tempfile.mkdtemp(prefix="jsign_"))
    entrada = Path(caminho_pdf_entrada)
    base = entrada.stem
    esperado = workdir / f"{base}_signed.pdf"

    cmd = [
        "java", "-jar", CAMINHO_JSIGNPDF,
        "-kst", "PKCS12",
        "-ksf", caminho_certificado,
        "-ksp", senha_certificado,
        "-d", str(workdir),
        "-op", "",
        "-os", "_signed"
    ]
    if assinatura_visivel:
        llx, lly, urx, ury = map(str, coords_assin)
        cmd += [
            "-V", "-pg", str(pagina),
            "-llx", llx, "-lly", lly, "-urx", urx, "-ury", ury,
            "--render-mode", "GRAPHIC_ONLY"
        ]
        if assinatura_img_path and os.path.exists(assinatura_img_path):
            cmd += ["--img-path", assinatura_img_path]

    cmd.append(str(entrada))
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"JSignPdf falhou (exit {proc.returncode}).\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")

    if esperado.exists():
        return str(esperado)
    candidates = list(workdir.glob(f"{base}_signed.pdf")) or list(workdir.glob("*_signed.pdf"))
    if candidates:
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return str(candidates[0])
    raise FileNotFoundError("PDF assinado não encontrado pelo JSignPdf.")

def buscar_paciente_api(paciente_id):
    try:
        import requests
        url = "https://medico.centroclinicomaster.com.br/api/registro/registro/idpaciente"
        r = requests.post(url, json={"idpaciente": paciente_id}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and data:
            data = data[0]
        return data.get("nomepaciente", ""), data.get("cpf", "")
    except Exception:
        return "", ""

@exames_bp.route('/files/pedidos/<path:filename>')
def servir_pedido(filename):
    full_path = os.path.join(PASTA_PEDIDOS, filename)
    if not os.path.isfile(full_path):
        return {"erro": "Arquivo não encontrado"}, 404
    return send_file(full_path, mimetype='application/pdf')

@exames_bp.route('/validar_pedido_exame/<int:pedido_id>')
def validar_pedido_exame(pedido_id: int):
    medico_id = request.args.get('mid', type=int)
    try:
        conn = _db_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT paciente_id,
                   COALESCE(nome_paciente, ''),
                   COALESCE(cpf_paciente,   ''),
                   exames, data_pedido,
                   COALESCE(pdf_assinado_path,'')
            FROM pedidos_exames
            WHERE id=%s
        """, (pedido_id,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return "Pedido não encontrado", 404

        paciente_id, nome_pac_db, cpf_pac_db, exames_texto, data_pedido, pdf_assinado_path = row

        nome_paciente = nome_pac_db or ""
        cpf_paciente  = cpf_pac_db  or ""

        if not nome_paciente or not cpf_paciente:
            try:
                cur.execute("SELECT nome, cpf FROM pacientes WHERE id=%s", (paciente_id,))
                pr = cur.fetchone()
                if pr:
                    if not nome_paciente: nome_paciente = pr[0] or ""
                    if not cpf_paciente:  cpf_paciente  = pr[1] or ""
            except Exception:
                pass

        if (not nome_paciente or not cpf_paciente) and paciente_id:
            n_api, cpf_api = buscar_paciente_api(paciente_id)
            if not nome_paciente: nome_paciente = n_api
            if not cpf_paciente:  cpf_paciente  = cpf_api

        nome_medico, assinatura_img_path = "", ""
        assinatura_data_uri = None
        conselho_label = "Registro profissional"
        crm = ""  # só para fallback
        if medico_id:
            try:
                cur.execute("SELECT nome, crm, COALESCE(assinatura_img_path,'') FROM medicos WHERE id=%s", (medico_id,))
                mr = cur.fetchone()
                if mr:
                    nome_medico = mr[0] or ""
                    crm = mr[1] or ""
                    assinatura_img_path = (mr[2] or "").strip()
                    # >>> usa a mesma lógica da receita
                    conselho_label = montar_conselho_label(int(medico_id), crm)
                    if assinatura_img_path and os.path.exists(assinatura_img_path):
                        mime = "image/png"
                        ext = assinatura_img_path.lower()
                        if ext.endswith(".jpg") or ext.endswith(".jpeg"):
                            mime = "image/jpeg"
                        with open(assinatura_img_path, "rb") as f:
                            b64 = base64.b64encode(f.read()).decode("ascii")
                        assinatura_data_uri = f"data:{mime};base64,{b64}"
            except Exception:
                pass

        cur.close(); conn.close()
    except Exception as e:
        current_app.logger.exception("Erro ao consultar para validação: %s", e)
        return "Erro interno", 500

    data_emissao_fmt = ""
    try:
        if isinstance(data_pedido, datetime):
            data_emissao_fmt = data_pedido.strftime("%d/%m/%Y")
        else:
            data_emissao_fmt = fmt_data(str(data_pedido))
    except Exception:
        pass

    pdf_nome = ""
    if pdf_assinado_path:
        pdf_nome = os.path.basename(str(pdf_assinado_path).strip().replace("\\", "/"))

    # >>> NÃO ENVIE 'crm' PARA O TEMPLATE — use só 'conselho_label'
    return render_template(
        "validar_pedido_exame.html",
        status=1,
        nome_medico=nome_medico,
        conselho_label=conselho_label,
        assinatura_data_uri=assinatura_data_uri,
        nome_paciente=nome_paciente,
        cpf_paciente=fmt_cpf(cpf_paciente),
        data_emissao=data_emissao_fmt,
        texto=f"Pedido de exames:\n{exames_texto or ''}",
        pdf_filename=pdf_nome
    )

@exames_bp.route('/api/gerar-pedido-exames', methods=['POST'])
def gerar_pedido_exames():
    try:
        data = request.get_json(force=True, silent=False)
    except Exception:
        return {"erro": "JSON inválido"}, 400

    medico_id        = data.get("medico_id")
    paciente_id      = data.get("id_paciente") or data.get("paciente_id")
    nome_paciente    = data.get("nome_paciente", "")
    cpf_paciente     = data.get("cpf_paciente", "")
    data_nascimento  = data.get("data_nascimento", "")
    exames_marcados  = data.get("lista_exames", [])
    outros           = data.get("outros_exames", "")

    if not medico_id or not isinstance(exames_marcados, list) or len(exames_marcados) == 0:
        return {"erro": "Campos obrigatórios ausentes (medico_id, lista_exames)"}, 400

    # --- dados do médico
    certificado_path, certificado_senha, assinatura_img_path, crm, nome_medico = obter_dados_medico(medico_id)
    certificado_path_clean  = _clean_path(certificado_path)
    assinatura_img_clean    = _clean_path(assinatura_img_path)
    certificado_senha_clean = (certificado_senha or "").strip()

    # --- rótulo do conselho (igual receita)
    conselho_label = montar_conselho_label(int(medico_id), crm)

    # --- papel (BACKEND)
    cfg_papel = _obter_cfg_papel(int(medico_id), doc_tipo="PEDIDO_EXAMES")
    tamanho_papel = cfg_papel["padrao"]
    if tamanho_papel == 'A5':
        pagesize = A5; papel_timbrado = cfg_papel["a5_path"]
    else:
        pagesize = A4; papel_timbrado = cfg_papel["a4_path"]

    largura, altura = pagesize
    ext_timbrado   = (os.path.splitext(papel_timbrado or "")[1] or "").lower()

    if papel_timbrado and not os.path.isfile(papel_timbrado):
        return {"erro": f"Papel timbrado ({tamanho_papel}) não encontrado: {papel_timbrado}"}, 400

    cpf_fmt  = fmt_cpf(cpf_paciente)
    nasc_fmt = fmt_data(data_nascimento)

    lista = [str(x) for x in exames_marcados]
    if outros:
        lista.append(f"Outros: {outros}")
    exames_texto = "\n".join(lista)
    agora_sql = datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')

    # cria registro pra obter ID
    try:
        conn = _db_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pedidos_exames
              (paciente_id, nome_paciente, cpf_paciente, exames, data_pedido, pdf_assinado_path)
            VALUES
              (%s, %s, %s, %s, %s, %s)
            """,
            (int(paciente_id or 0), nome_paciente, cpf_fmt, exames_texto, agora_sql, None)
        )
        pedido_id = cur.lastrowid
        cur.close(); conn.close()
    except Exception as e:
        current_app.logger.exception("Falha INSERT pedidos_exames: %s", e)
        return {"erro": f"Falha ao salvar pedido no banco: {e}"}, 500

    ts = datetime.now(TZ).strftime('%Y%m%d_%H%M%S')
    nome_arquivo = secure_filename(f"pedido_exames_{medico_id}_{ts}.pdf")
    base = PUBLIC_BASE_URL if PUBLIC_BASE_URL else request.url_root.rstrip('/')
    url_publica_arquivo = f"{base}/files/pedidos/{nome_arquivo}"
    url_validacao = f"{base}/validar_pedido_exame/{pedido_id}?mid={medico_id}"
    destino = os.path.join(PASTA_PEDIDOS, nome_arquivo)

    # ---------- 1) Conteúdo ----------
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=pagesize)

    usar_imagem_fundo = papel_timbrado and ext_timbrado in (".png", ".jpg", ".jpeg")
    if usar_imagem_fundo:
        _desenhar_fundo_imagem(pdf, papel_timbrado, largura, altura)

    y = _desenhar_cabecalho(pdf, largura, altura,
                            nome_paciente or "Paciente", cpf_fmt, nasc_fmt,
                            tamanho_papel=tamanho_papel)

    pdf.setFont("Helvetica", 11)
    for exame in exames_marcados:
        if y < 80:
            pdf.showPage()
            if usar_imagem_fundo:
                _desenhar_fundo_imagem(pdf, papel_timbrado, largura, altura)
            y = _desenhar_cabecalho(pdf, largura, altura,
                                    nome_paciente or "Paciente", cpf_fmt, nasc_fmt,
                                    tamanho_papel=tamanho_papel)
        pdf.drawString(60, y, f"- {exame}")
        y -= 18

    if outros:
        if y < 80:
            pdf.showPage()
            if usar_imagem_fundo:
                _desenhar_fundo_imagem(pdf, papel_timbrado, largura, altura)
            y = _desenhar_cabecalho(pdf, largura, altura,
                                    nome_paciente or "Paciente", cpf_fmt, nasc_fmt,
                                    tamanho_papel=tamanho_papel)
        pdf.drawString(60, y, f"Outros: {outros}")

    # ===== Rodapé =====
    has_cert = bool(certificado_path_clean and certificado_senha_clean and os.path.isfile(certificado_path_clean))

    if has_cert:
        linha_centro_y = 135
        qr_size = 60
        gap = 14

        header_size = 11 if tamanho_papel == 'A4' else 10
        body_size   = 9  if tamanho_papel == 'A4' else 8

        # usa o rótulo dinâmico do conselho
        header_text = f"{nome_medico}    |    {conselho_label}"
        header_w = pdf.stringWidth(header_text, "Helvetica-Bold", header_size)
        body_w_1  = pdf.stringWidth("Para verificar a autenticidade, leia o QR code ao lado.", "Helvetica", body_size)
        body_w_2  = pdf.stringWidth("Assinatura digital válida conforme MP 2.200-2/2001", "Helvetica", body_size)

        assinatura_temp_path = None
        assinatura_w = assinatura_h = 0
        if assinatura_img_clean and os.path.exists(assinatura_img_clean) and assinatura_img_clean.lower().endswith(('.png','.jpg','.jpeg')):
            try:
                img = Image.open(assinatura_img_clean)
                if img.mode != "RGBA":
                    img = img.convert("RGBA")
                bbox = img.getbbox()
                if bbox:
                    img = img.crop(bbox)
                img_w, img_h = img.size
                max_w = 330 if tamanho_papel == 'A4' else 260
                max_h = 75  if tamanho_papel == 'A4' else 54
                ratio = min(max_w / img_w, max_h / img_h, 1.0)
                assinatura_w = int(img_w * ratio)
                assinatura_h = int(img_h * ratio)
                assinatura_temp_path = os.path.join(tempfile.gettempdir(), "assinatura_tmp_pedido.png")
                img.resize((assinatura_w, assinatura_h), Image.LANCZOS).save(assinatura_temp_path)
            except Exception as e:
                current_app.logger.warning("Falha ao preparar assinatura imagem: %s", e)
                assinatura_temp_path = None
                assinatura_w = assinatura_h = 0

        text_block_w = max(header_w, body_w_1, body_w_2, assinatura_w)
        total_w = qr_size + gap + text_block_w

        x_start = (largura - total_w) / 2.0
        qr_x = x_start
        qr_y = linha_centro_y - qr_size // 2
        text_x = qr_x + qr_size + gap

        qr_buffer = gerar_qrcode(url_validacao)
        pdf.drawImage(ImageReader(qr_buffer), qr_x, qr_y, qr_size, qr_size, mask='auto')

        if assinatura_temp_path:
            margem_dir = largura - 50
            if text_x + assinatura_w > margem_dir:
                assinatura_w = max(60, int(margem_dir - text_x))
                assinatura_h = int(assinatura_h * (assinatura_w / max(assinatura_w, 1)))
            y_nome = linha_centro_y + 15
            pdf.drawImage(assinatura_temp_path, text_x, y_nome + 10,
                          width=assinatura_w, height=assinatura_h, mask='auto')
            try:
                os.remove(assinatura_temp_path)
            except:
                pass

        pdf.setFont("Helvetica-Bold", header_size)
        pdf.drawString(text_x, linha_centro_y + 15, header_text)
        pdf.setFont("Helvetica", body_size)
        pdf.drawString(text_x, linha_centro_y, "Para verificar a autenticidade, leia o QR code ao lado.")
        pdf.drawString(text_x, linha_centro_y - 15, "Assinatura digital válida conforme MP 2.200-2/2001")
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

    tmp_conteudo = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    with open(tmp_conteudo.name, 'wb') as f:
        f.write(buffer.read())

    # ---------- 2) Merge com timbrado PDF (se for PDF) ----------
    final_sem_assinatura = tmp_conteudo.name
    tmp_merged = None
    try:
        if papel_timbrado and ext_timbrado == ".pdf":
            tmp_merged = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
            _merge_with_bg_as_base(tmp_conteudo.name, papel_timbrado, tmp_merged.name)
            final_sem_assinatura = tmp_merged.name
    finally:
        try:
            os.unlink(tmp_conteudo.name)
        except:
            pass

    # ---------- 3) Assinatura digital INVISÍVEL ----------
    final_assinado_tmp = final_sem_assinatura
    try:
        if has_cert:
            final_assinado_tmp = assinar_pdf_jsignpdf(
                caminho_pdf_entrada=final_sem_assinatura,
                caminho_certificado=certificado_path_clean,
                senha_certificado=certificado_senha_clean,
                assinatura_img_path=assinatura_img_clean,
                assinatura_visivel=False,
                coords_assin=_calc_sig_rect_below_qr(tamanho_papel),
                pagina=1
            )
        else:
            current_app.logger.info("Certificado ausente/inválido - retornando PDF sem assinatura digital.")
    except Exception as e:
        current_app.logger.exception("Falha no JSignPdf: %s", e)

    # ---------- 4) Salva arquivo final ----------
    with open(final_assinado_tmp, 'rb') as src, open(destino, 'wb') as dst:
        dst.write(src.read())

    try:
        if os.path.exists(final_sem_assinatura) and os.path.abspath(final_sem_assinatura) != os.path.abspath(final_assinado_tmp):
            os.unlink(final_sem_assinatura)
    except:
        pass
    if tmp_merged and os.path.exists(tmp_merged.name):
        try:
            os.unlink(tmp_merged.name)
        except:
            pass
    try:
        tmp_parent = os.path.dirname(final_assinado_tmp)
        if "jsign_" in tmp_parent and os.path.exists(final_assinado_tmp):
            os.unlink(final_assinado_tmp)
            try:
                Path(tmp_parent).rmdir()
            except:
                pass
    except:
        pass

    # ---------- 5) Atualiza caminho no banco ----------
    try:
        conn = _db_conn()
        cur = conn.cursor()
        cur.execute("UPDATE pedidos_exames SET pdf_assinado_path=%s WHERE id=%s",
                    (destino.replace('\\', '/'), pedido_id))
        cur.close(); conn.close()
    except Exception as e:
        current_app.logger.exception("UPDATE caminho PDF falhou: %s", e)

    # ---------- 6) Resposta ----------
    return jsonify(
        status="ok",
        url=url_publica_arquivo,
        pedido_id=pedido_id,
        validar=f"{base}/validar_pedido_exame/{pedido_id}?mid={medico_id}",
        caminho=destino.replace('\\', '/')
    )
