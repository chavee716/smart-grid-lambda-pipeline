from pathlib import Path
import re

from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak

root = Path(__file__).parent
source = root / "demo_script.md"
output = root / "smart_grid_demo_script.pdf"

styles = getSampleStyleSheet()
styles.add(ParagraphStyle(name="TitleCenter", parent=styles["Title"], alignment=TA_CENTER, fontSize=20, leading=25, spaceAfter=12))
styles.add(ParagraphStyle(name="H1Custom", parent=styles["Heading1"], fontSize=15, leading=19, spaceBefore=12, spaceAfter=8, keepWithNext=True))
styles.add(ParagraphStyle(name="H2Custom", parent=styles["Heading2"], fontSize=12, leading=15, spaceBefore=9, spaceAfter=5, keepWithNext=True))
styles.add(ParagraphStyle(name="BodyCustom", parent=styles["BodyText"], fontSize=9.5, leading=13, spaceAfter=6))
styles.add(ParagraphStyle(name="QuoteCustom", parent=styles["BodyText"], leftIndent=10, rightIndent=5, fontSize=9.5, leading=13, spaceAfter=4, textColor="#243447"))
styles.add(ParagraphStyle(name="CodeCustom", parent=styles["Code"], fontSize=8.5, leading=11, leftIndent=10, spaceAfter=6))


def escape(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def inline(text):
    text = escape(text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"`(.+?)`", r"<font name='Courier'>\1</font>", text)
    return text


def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColorRGB(0.4, 0.4, 0.4)
    canvas.drawCentredString(A4[0] / 2, 10 * mm, f"Smart Grid Demo Script  |  {doc.page}")
    canvas.restoreState()

story = []
lines = source.read_text(encoding="utf-8").splitlines()
in_code = False
code_lines = []
for line in lines:
    if line.startswith("```"):
        if in_code:
            story.append(Paragraph(escape("\n".join(code_lines)), styles["CodeCustom"]))
            story.append(Spacer(1, 3))
            code_lines = []
        in_code = not in_code
        continue
    if in_code:
        code_lines.append(line)
        continue
    if not line.strip():
        continue
    if line.startswith("# "):
        story.append(Paragraph(inline(line[2:]), styles["TitleCenter"]))
    elif line.startswith("## "):
        story.append(Paragraph(inline(line[3:]), styles["H1Custom"]))
    elif line.startswith("### "):
        story.append(Paragraph(inline(line[4:]), styles["H2Custom"]))
    elif line.startswith("> "):
        story.append(Paragraph(inline(line[2:]), styles["QuoteCustom"]))
    elif line.startswith("- [ ] "):
        story.append(Paragraph("&#9744; " + inline(line[6:]), styles["BodyCustom"]))
    elif line.startswith("- "):
        story.append(Paragraph("&#8226; " + inline(line[2:]), styles["BodyCustom"]))
    elif re.match(r"^\d+\. ", line):
        story.append(Paragraph(inline(line), styles["BodyCustom"]))
    else:
        story.append(Paragraph(inline(line), styles["BodyCustom"]))

doc = SimpleDocTemplate(str(output), pagesize=A4, rightMargin=18 * mm, leftMargin=18 * mm, topMargin=16 * mm, bottomMargin=17 * mm, title="Smart Grid Demo Script")
doc.build(story, onFirstPage=footer, onLaterPages=footer)
print(output)
