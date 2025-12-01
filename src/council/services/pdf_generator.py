"""
PDF Generator Service for Virtual Judicial Council deliberations.

Generates professional PDF documents containing:
- Case information and summary
- Similar cases for reference
- Full deliberation transcript
- Legal opinion (if generated)
"""

import io
import logging
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from src.council.schemas import (
    AgentId,
    CaseInput,
    DeliberationMessage,
    LegalOpinionDraft,
    SimilarCase,
)

logger = logging.getLogger(__name__)


# Agent display names in Indonesian
AGENT_NAMES = {
    AgentId.STRICT: "Hakim Konstruksionis (Strict)",
    AgentId.HUMANIST: "Hakim Humanis",
    AgentId.HISTORIAN: "Hakim Sejarawan",
}


class PDFGeneratorService:
    """Service for generating PDF documents from deliberation sessions."""

    def __init__(self):
        """Initialize the PDF generator with styles."""
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()

    def _setup_custom_styles(self) -> None:
        """Set up custom paragraph styles for the PDF."""
        # Title style
        self.styles.add(
            ParagraphStyle(
                name="CustomTitle",
                parent=self.styles["Heading1"],
                fontSize=18,
                spaceAfter=20,
                alignment=1,  # Center
                textColor=colors.HexColor("#1a365d"),
            )
        )

        # Section header style
        self.styles.add(
            ParagraphStyle(
                name="SectionHeader",
                parent=self.styles["Heading2"],
                fontSize=14,
                spaceBefore=15,
                spaceAfter=10,
                textColor=colors.HexColor("#2c5282"),
                borderPadding=(0, 0, 5, 0),
            )
        )

        # Subsection style
        self.styles.add(
            ParagraphStyle(
                name="SubSection",
                parent=self.styles["Heading3"],
                fontSize=12,
                spaceBefore=10,
                spaceAfter=5,
                textColor=colors.HexColor("#2d3748"),
            )
        )

        # Body text style - override default
        self.styles["BodyText"].fontSize = 10
        self.styles["BodyText"].spaceBefore = 5
        self.styles["BodyText"].spaceAfter = 5
        self.styles["BodyText"].leading = 14

        # Judge message style
        self.styles.add(
            ParagraphStyle(
                name="JudgeMessage",
                parent=self.styles["Normal"],
                fontSize=10,
                spaceBefore=5,
                spaceAfter=10,
                leading=14,
                leftIndent=10,
                borderPadding=10,
            )
        )

        # Judge name style
        self.styles.add(
            ParagraphStyle(
                name="JudgeName",
                parent=self.styles["Normal"],
                fontSize=11,
                spaceBefore=10,
                spaceAfter=3,
                textColor=colors.HexColor("#2c5282"),
                fontName="Helvetica-Bold",
            )
        )

        # Footer style
        self.styles.add(
            ParagraphStyle(
                name="Footer",
                parent=self.styles["Normal"],
                fontSize=8,
                textColor=colors.gray,
                alignment=1,
            )
        )

    def generate_deliberation_pdf(
        self,
        session_id: str,
        case_input: CaseInput,
        similar_cases: list[SimilarCase],
        messages: list[DeliberationMessage],
        legal_opinion: LegalOpinionDraft | None = None,
    ) -> bytes:
        """
        Generate a PDF document for a deliberation session.

        Args:
            session_id: Session ID
            case_input: Case information
            similar_cases: Similar cases for reference
            messages: Deliberation messages
            legal_opinion: Generated legal opinion (optional)

        Returns:
            PDF document as bytes
        """
        buffer = io.BytesIO()

        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=2 * cm,
            leftMargin=2 * cm,
            topMargin=2 * cm,
            bottomMargin=2 * cm,
        )

        story = []

        # Title
        story.append(
            Paragraph(
                "Hasil Musyawarah Lexicon Judge Council",
                self.styles["CustomTitle"],
            )
        )
        story.append(
            Paragraph(
                f"Tanggal: {datetime.now().strftime('%d %B %Y')}",
                self.styles["Footer"],
            )
        )
        story.append(Spacer(1, 20))

        # Case Information Section
        story.extend(self._build_case_section(case_input))

        # Similar Cases Section
        if similar_cases:
            story.extend(self._build_similar_cases_section(similar_cases))

        # Deliberation Section
        story.extend(self._build_deliberation_section(messages))

        # Legal Opinion Section
        if legal_opinion:
            story.extend(self._build_opinion_section(legal_opinion))

        # Footer
        story.append(Spacer(1, 30))
        story.append(
            Paragraph(
                f"Session ID: {session_id}",
                self.styles["Footer"],
            )
        )
        story.append(
            Paragraph(
                "Dokumen ini dihasilkan oleh Lexicon Judge Council",
                self.styles["Footer"],
            )
        )

        doc.build(story)

        pdf_bytes = buffer.getvalue()
        buffer.close()

        logger.info(f"Generated PDF for session {session_id}: {len(pdf_bytes)} bytes")
        return pdf_bytes

    def _build_case_section(self, case_input: CaseInput) -> list:
        """Build the case information section."""
        elements = []

        elements.append(
            Paragraph("Informasi Perkara", self.styles["SectionHeader"])
        )

        # Case summary
        elements.append(
            Paragraph(
                f"<b>Ringkasan:</b> {case_input.parsed_case.summary}",
                self.styles["BodyText"],
            )
        )

        # Case type
        elements.append(
            Paragraph(
                f"<b>Jenis Perkara:</b> {case_input.parsed_case.case_type.value}",
                self.styles["BodyText"],
            )
        )

        # Defendant profile
        if case_input.parsed_case.defendant_profile:
            profile = case_input.parsed_case.defendant_profile
            profile_text = []
            if profile.age:
                profile_text.append(f"Usia: {profile.age} tahun")
            if profile.occupation:
                profile_text.append(f"Pekerjaan: {profile.occupation}")
            profile_text.append(
                f"Pelanggar Pertama: {'Ya' if profile.is_first_offender else 'Tidak'}"
            )
            elements.append(
                Paragraph(
                    f"<b>Profil Terdakwa:</b> {', '.join(profile_text)}",
                    self.styles["BodyText"],
                )
            )

        # Key facts
        if case_input.parsed_case.key_facts:
            elements.append(
                Paragraph("<b>Fakta Kunci:</b>", self.styles["BodyText"])
            )
            for fact in case_input.parsed_case.key_facts:
                elements.append(
                    Paragraph(f"• {fact}", self.styles["BodyText"])
                )

        # Charges
        if case_input.parsed_case.charges:
            elements.append(
                Paragraph(
                    f"<b>Dakwaan:</b> {', '.join(case_input.parsed_case.charges)}",
                    self.styles["BodyText"],
                )
            )

        # Corruption details
        if case_input.parsed_case.corruption:
            corruption = case_input.parsed_case.corruption
            elements.append(
                Paragraph(
                    f"<b>Kerugian Negara:</b> Rp {corruption.state_loss_idr:,.0f}",
                    self.styles["BodyText"],
                )
            )
            if corruption.position:
                elements.append(
                    Paragraph(
                        f"<b>Jabatan:</b> {corruption.position}",
                        self.styles["BodyText"],
                    )
                )

        # Narcotics details
        if case_input.parsed_case.narcotics:
            narcotics = case_input.parsed_case.narcotics
            elements.append(
                Paragraph(
                    f"<b>Jenis Narkotika:</b> {narcotics.substance}",
                    self.styles["BodyText"],
                )
            )
            elements.append(
                Paragraph(
                    f"<b>Berat:</b> {narcotics.weight_grams} gram",
                    self.styles["BodyText"],
                )
            )

        elements.append(Spacer(1, 10))
        return elements

    def _build_similar_cases_section(
        self, similar_cases: list[SimilarCase]
    ) -> list:
        """Build the similar cases section."""
        elements = []

        elements.append(
            Paragraph("Perkara Serupa", self.styles["SectionHeader"])
        )

        # Create table data
        table_data = [
            ["No. Perkara", "Putusan", "Hukuman", "Kemiripan"]
        ]

        for case in similar_cases:
            verdict = case.verdict_summary
            if len(verdict) > 50:
                verdict = verdict[:50] + "..."
            table_data.append([
                case.case_number,
                verdict,
                f"{case.sentence_months} bulan",
                f"{case.similarity_score:.0%}",
            ])

        table = Table(table_data, colWidths=[4 * cm, 6 * cm, 2.5 * cm, 2 * cm])
        table.setStyle(
            TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2c5282")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 9),
                ("FONTSIZE", (0, 1), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 1), (-1, -1), 5),
                ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#f7fafc")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ])
        )

        elements.append(table)
        elements.append(Spacer(1, 15))
        return elements

    def _build_deliberation_section(
        self, messages: list[DeliberationMessage]
    ) -> list:
        """Build the deliberation transcript section."""
        elements = []

        elements.append(
            Paragraph("Transkrip Musyawarah", self.styles["SectionHeader"])
        )

        for i, msg in enumerate(messages, 1):
            # Determine sender name
            if hasattr(msg.sender, "agent_id"):
                sender_name = AGENT_NAMES.get(
                    msg.sender.agent_id,
                    msg.sender.agent_id.value.title(),
                )
                sender_color = self._get_agent_color(msg.sender.agent_id)
            elif msg.sender.type == "user":
                sender_name = "Pengguna"
                sender_color = "#718096"
            else:
                sender_name = "Sistem"
                sender_color = "#a0aec0"

            # Add sender name with styling
            elements.append(
                Paragraph(
                    f"<font color='{sender_color}'><b>{sender_name}</b></font>",
                    self.styles["JudgeName"],
                )
            )

            # Add message content (escape HTML special characters)
            content = (
                msg.content
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\n", "<br/>")
            )
            elements.append(
                Paragraph(content, self.styles["JudgeMessage"])
            )

            # Add separator between messages
            if i < len(messages):
                elements.append(Spacer(1, 5))

        elements.append(Spacer(1, 15))
        return elements

    def _build_opinion_section(self, opinion: LegalOpinionDraft) -> list:
        """Build the legal opinion section."""
        elements = []

        elements.append(
            Paragraph("Pendapat Hukum", self.styles["SectionHeader"])
        )

        # Verdict recommendation
        elements.append(
            Paragraph("Rekomendasi Putusan", self.styles["SubSection"])
        )
        verdict = opinion.verdict_recommendation
        elements.append(
            Paragraph(
                f"<b>Keputusan:</b> {verdict.decision.value}",
                self.styles["BodyText"],
            )
        )
        elements.append(
            Paragraph(
                f"<b>Tingkat Keyakinan:</b> {verdict.confidence}",
                self.styles["BodyText"],
            )
        )
        elements.append(
            Paragraph(
                f"<b>Alasan:</b> {verdict.reasoning}",
                self.styles["BodyText"],
            )
        )

        # Sentence recommendation
        elements.append(
            Paragraph("Rekomendasi Hukuman", self.styles["SubSection"])
        )
        sentence = opinion.sentence_recommendation
        elements.append(
            Paragraph(
                f"<b>Penjara:</b> {sentence.imprisonment_months.minimum} - "
                f"{sentence.imprisonment_months.maximum} bulan "
                f"(rekomendasi: {sentence.imprisonment_months.recommended} bulan)",
                self.styles["BodyText"],
            )
        )
        elements.append(
            Paragraph(
                f"<b>Denda:</b> Rp {sentence.fine_idr.minimum:,.0f} - "
                f"Rp {sentence.fine_idr.maximum:,.0f} "
                f"(rekomendasi: Rp {sentence.fine_idr.recommended:,.0f})",
                self.styles["BodyText"],
            )
        )

        if sentence.additional_penalties:
            penalties = ", ".join(sentence.additional_penalties)
            elements.append(
                Paragraph(
                    f"<b>Hukuman Tambahan:</b> {penalties}",
                    self.styles["BodyText"],
                )
            )

        # Legal arguments
        if opinion.legal_arguments:
            elements.append(
                Paragraph("Argumen Hukum", self.styles["SubSection"])
            )

            if opinion.legal_arguments.for_conviction:
                elements.append(
                    Paragraph("<b>Untuk Pemidanaan:</b>", self.styles["BodyText"])
                )
                for arg in opinion.legal_arguments.for_conviction:
                    elements.append(
                        Paragraph(
                            f"• {arg.argument} ({arg.source_agent.value})",
                            self.styles["BodyText"],
                        )
                    )

            if opinion.legal_arguments.for_leniency:
                elements.append(
                    Paragraph("<b>Untuk Keringanan:</b>", self.styles["BodyText"])
                )
                for arg in opinion.legal_arguments.for_leniency:
                    elements.append(
                        Paragraph(
                            f"• {arg.argument} ({arg.source_agent.value})",
                            self.styles["BodyText"],
                        )
                    )

        # Dissenting views
        if opinion.dissenting_views:
            elements.append(
                Paragraph("Pandangan Berbeda", self.styles["SubSection"])
            )
            for view in opinion.dissenting_views:
                elements.append(
                    Paragraph(f"• {view}", self.styles["BodyText"])
                )

        elements.append(Spacer(1, 15))
        return elements

    def _get_agent_color(self, agent_id: AgentId) -> str:
        """Get the color associated with an agent."""
        colors_map = {
            AgentId.STRICT: "#c53030",      # Red
            AgentId.HUMANIST: "#2f855a",    # Green
            AgentId.HISTORIAN: "#2b6cb0",   # Blue
        }
        return colors_map.get(agent_id, "#4a5568")


# =============================================================================
# Singleton
# =============================================================================

_pdf_generator: PDFGeneratorService | None = None


def get_pdf_generator_service() -> PDFGeneratorService:
    """Get or create the PDF generator service singleton."""
    global _pdf_generator
    if _pdf_generator is None:
        _pdf_generator = PDFGeneratorService()
    return _pdf_generator
