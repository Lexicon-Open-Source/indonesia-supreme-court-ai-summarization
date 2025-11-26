"""
LLM Extraction module for processing Indonesian Supreme Court documents.

This module handles:
1. PDF to text conversion (uses existing io.py functions)
2. Text cleaning (uses existing io.py functions)
3. Text chunking (100 pages per chunk)
4. LLM extraction with structured output
5. Database persistence to llm_extractions table
"""

import json
import logging
import traceback
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import litellm
from litellm import acompletion
from pydantic import BaseModel, Field

# Enable JSON schema validation for structured output
litellm.enable_json_schema_validation = True
# Uncomment for verbose debugging:
# litellm.set_verbose = True
from sqlalchemy import TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel import Column, Field as SQLField, SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

logger = logging.getLogger(__name__)

# LLM Model configuration
MODEL = "gemini/gemini-2.5-flash-lite"
CHUNK_SIZE = 100  # Number of pages per chunk


# =============================================================================
# Enums
# =============================================================================


class Gender(str, Enum):
    MALE = "Laki-laki"
    FEMALE = "Perempuan"


class IndictmentType(str, Enum):
    CAMPURAN = "Campuran"
    ALTERNATIF = "Alternatif"
    SUBSIDIAIR = "Subsidiair"
    KUMULATIF = "Kumulatif"
    TUNGGAL = "Tunggal"


class ExceptionStatus(str, Enum):
    DITOLAK = "Ditolak"
    DITERIMA = "Diterima"
    TIDAK_ADA = "Tidak Ada"


class VerdictResult(str, Enum):
    GUILTY = "guilty"  # Terbukti bersalah
    PARTIALLY_GUILTY = "partially_guilty"  # Sebagian terbukti
    NOT_GUILTY = "not_guilty"  # Bebas
    ACQUITTED = "acquitted"  # Lepas dari segala tuntutan


class ConfinementType(str, Enum):
    KURUNGAN = "kurungan"
    PENJARA = "penjara"


# =============================================================================
# Nested Models - Address
# =============================================================================


class StructuredAddress(BaseModel):
    """Structured address information."""

    street: str | None = Field(
        default=None,
        description="Nama jalan dan nomor rumah (contoh: Jl. Putri Tujuh No. 06)",
    )
    rt_rw: str | None = Field(
        default=None, description="RT/RW (contoh: RT 016 RW 005)"
    )
    kelurahan: str | None = Field(
        default=None, description="Nama kelurahan/desa"
    )
    kecamatan: str | None = Field(default=None, description="Nama kecamatan")
    city: str | None = Field(
        default=None, description="Nama kota/kabupaten"
    )
    province: str | None = Field(default=None, description="Nama provinsi")
    full_address: str | None = Field(
        default=None,
        description="Alamat lengkap dalam satu string (jika tidak bisa dipecah)",
    )


# =============================================================================
# Nested Models - Defendant
# =============================================================================


class DefendantInfo(BaseModel):
    """Complete defendant information."""

    name: str | None = Field(default=None, description="Nama lengkap terdakwa")
    alias: str | None = Field(
        default=None, description="Nama alias/panggilan terdakwa jika ada"
    )
    place_of_birth: str | None = Field(
        default=None, description="Kota atau tempat kelahiran terdakwa"
    )
    date_of_birth: str | None = Field(
        default=None, description="Tanggal lahir terdakwa (Format: YYYY-MM-DD)"
    )
    age: int | None = Field(default=None, description="Umur terdakwa saat putusan")
    gender: str | None = Field(
        default=None, description="Jenis kelamin terdakwa (Laki-laki/Perempuan)"
    )
    citizenship: str | None = Field(
        default=None, description="Status kewarganegaraan terdakwa"
    )
    address: StructuredAddress | None = Field(
        default=None, description="Alamat lengkap terdakwa yang terstruktur"
    )
    religion: str | None = Field(default=None, description="Agama terdakwa")
    occupation: str | None = Field(default=None, description="Pekerjaan terdakwa")
    education: str | None = Field(
        default=None, description="Tingkat pendidikan terakhir terdakwa"
    )


# =============================================================================
# Nested Models - Legal Counsel
# =============================================================================


class LegalCounsel(BaseModel):
    """Legal counsel/lawyer information."""

    name: str | None = Field(default=None, description="Nama penasihat hukum")
    office_name: str | None = Field(
        default=None, description="Nama kantor hukum"
    )
    office_address: str | None = Field(
        default=None, description="Alamat kantor hukum"
    )


# =============================================================================
# Nested Models - Court Information
# =============================================================================


class CourtInfo(BaseModel):
    """Court and case information."""

    case_register_number: str | None = Field(
        default=None,
        description="Nomor register perkara pada pengadilan (Case Register Number)",
    )
    verdict_number: str | None = Field(
        default=None, description="Nomor putusan pengadilan"
    )
    court_name: str | None = Field(
        default=None, description="Nama Pengadilan Negeri/Tinggi"
    )
    court_level: str | None = Field(
        default=None,
        description="Tingkat pengadilan (Pengadilan Negeri/Pengadilan Tinggi/Mahkamah Agung)",
    )
    province: str | None = Field(
        default=None, description="Provinsi lokasi pengadilan"
    )
    city: str | None = Field(
        default=None, description="Kota lokasi pengadilan"
    )


# =============================================================================
# Nested Models - Court Personnel
# =============================================================================


class Judge(BaseModel):
    """Judge information."""

    name: str | None = Field(default=None, description="Nama hakim")
    role: str | None = Field(
        default=None,
        description="Peran hakim (Ketua Majelis/Hakim Anggota)",
    )


class CourtPersonnel(BaseModel):
    """All court personnel involved in the case."""

    judges: list[Judge] | None = Field(
        default=None, description="Daftar hakim yang menangani perkara"
    )
    prosecutors: list[str] | None = Field(
        default=None, description="Nama-nama Jaksa Penuntut Umum"
    )
    court_clerks: list[str] | None = Field(
        default=None, description="Nama-nama Panitera Pengganti"
    )


# =============================================================================
# Nested Models - Crime Period
# =============================================================================


class CrimePeriod(BaseModel):
    """Crime time period information."""

    start_date: str | None = Field(
        default=None, description="Tanggal mulai kejadian (Format: YYYY-MM-DD)"
    )
    end_date: str | None = Field(
        default=None, description="Tanggal akhir kejadian (Format: YYYY-MM-DD)"
    )
    description: str | None = Field(
        default=None,
        description="Deskripsi waktu kejadian dalam teks asli dokumen",
    )


# =============================================================================
# Nested Models - Cited Article
# =============================================================================


class CitedArticle(BaseModel):
    """Structured article citation."""

    article: str | None = Field(
        default=None, description="Nomor pasal (contoh: Pasal 2 Ayat (1))"
    )
    law_name: str | None = Field(
        default=None,
        description="Nama undang-undang (contoh: UU Pemberantasan Tindak Pidana Korupsi)",
    )
    law_number: str | None = Field(
        default=None, description="Nomor undang-undang (contoh: 31)"
    )
    law_year: int | None = Field(
        default=None, description="Tahun undang-undang (contoh: 1999)"
    )
    full_citation: str | None = Field(
        default=None,
        description="Kutipan lengkap pasal dalam satu string",
    )


# =============================================================================
# Nested Models - Indictment
# =============================================================================


class Indictment(BaseModel):
    """Indictment (dakwaan) information."""

    type: str | None = Field(
        default=None,
        description="Bentuk surat dakwaan (Campuran/Alternatif/Subsidiair/Kumulatif/Tunggal)",
    )
    chronology: str | None = Field(
        default=None, description="Ringkasan kronologis kasus dalam dakwaan"
    )
    crime_location: str | None = Field(
        default=None, description="Tempat kejadian perkara (Locus Delicti)"
    )
    crime_period: CrimePeriod | None = Field(
        default=None, description="Waktu kejadian perkara (Tempus Delicti)"
    )
    cited_articles: list[CitedArticle] | None = Field(
        default=None, description="Daftar pasal yang didakwakan secara terstruktur"
    )
    defense_exception_status: str | None = Field(
        default=None, description="Status eksepsi (Ditolak/Diterima/Tidak Ada)"
    )


# =============================================================================
# Nested Models - Prosecution Demand
# =============================================================================


class ProsecutionDemand(BaseModel):
    """Prosecution demand (tuntutan) information."""

    date: str | None = Field(
        default=None, description="Tanggal pembacaan tuntutan JPU (Format: YYYY-MM-DD)"
    )
    articles: list[CitedArticle] | None = Field(
        default=None, description="Pasal-pasal yang digunakan dalam tuntutan"
    )
    content: str | None = Field(
        default=None, description="Isi ringkas tuntutan Jaksa"
    )
    prison_sentence_months: float | None = Field(
        default=None, description="Lama penjara yang dituntut (dalam bulan)"
    )
    prison_sentence_description: str | None = Field(
        default=None,
        description="Deskripsi hukuman penjara yang dituntut (contoh: 2 tahun 6 bulan)",
    )
    fine_amount: float | None = Field(
        default=None, description="Nilai denda yang dituntut (dalam Rupiah)"
    )
    fine_subsidiary_confinement_months: int | None = Field(
        default=None,
        description="Durasi kurungan pengganti jika denda tidak dibayar (dalam bulan)",
    )
    restitution_amount: float | None = Field(
        default=None, description="Nilai uang pengganti yang dituntut (dalam Rupiah)"
    )
    restitution_subsidiary_type: str | None = Field(
        default=None,
        description="Jenis pidana pengganti jika uang pengganti tidak dibayar (kurungan/penjara)",
    )
    restitution_subsidiary_duration_months: int | None = Field(
        default=None,
        description="Durasi penjara/kurungan pengganti jika uang pengganti tidak dibayar (dalam bulan)",
    )


# =============================================================================
# Nested Models - Verdict Sentences
# =============================================================================


class ImprisonmentSentence(BaseModel):
    """Imprisonment sentence details."""

    duration_months: int | None = Field(
        default=None, description="Durasi hukuman penjara dalam bulan"
    )
    description: str | None = Field(
        default=None,
        description="Deskripsi hukuman penjara (contoh: 1 tahun 4 bulan)",
    )


class FineSentence(BaseModel):
    """Fine sentence details."""

    amount: float | None = Field(
        default=None, description="Nilai denda yang dijatuhkan (dalam Rupiah)"
    )
    subsidiary_confinement_months: int | None = Field(
        default=None,
        description="Durasi kurungan pengganti jika denda tidak dibayar (dalam bulan)",
    )


class RestitutionSentence(BaseModel):
    """Restitution (uang pengganti) sentence details."""

    amount: float | None = Field(
        default=None, description="Nilai uang pengganti yang diputus (dalam Rupiah)"
    )
    already_paid: float | None = Field(
        default=None, description="Jumlah yang sudah dibayar/dikembalikan (dalam Rupiah)"
    )
    remaining: float | None = Field(
        default=None, description="Sisa yang wajib dibayar (dalam Rupiah)"
    )
    subsidiary_type: str | None = Field(
        default=None,
        description="Jenis pidana pengganti (kurungan/penjara)",
    )
    subsidiary_duration_months: int | None = Field(
        default=None,
        description="Durasi penjara/kurungan pengganti jika tidak dibayar (dalam bulan)",
    )


class VerdictSentences(BaseModel):
    """All sentences in the verdict."""

    imprisonment: ImprisonmentSentence | None = Field(
        default=None, description="Detail hukuman penjara"
    )
    fine: FineSentence | None = Field(
        default=None, description="Detail hukuman denda"
    )
    restitution: RestitutionSentence | None = Field(
        default=None, description="Detail uang pengganti"
    )


# =============================================================================
# Nested Models - Verdict
# =============================================================================


class Verdict(BaseModel):
    """Complete verdict (putusan) information."""

    number: str | None = Field(
        default=None, description="Nomor putusan pengadilan"
    )
    date: str | None = Field(
        default=None, description="Tanggal putusan dibacakan (Format: YYYY-MM-DD)"
    )
    day: str | None = Field(default=None, description="Hari pembacaan putusan")
    year: int | None = Field(default=None, description="Tahun putusan")
    result: str | None = Field(
        default=None,
        description="Hasil putusan (guilty/partially_guilty/not_guilty/acquitted)",
    )
    primary_charge_proven: bool | None = Field(
        default=None, description="Apakah dakwaan primer terbukti"
    )
    subsidiary_charge_proven: bool | None = Field(
        default=None, description="Apakah dakwaan subsidiair terbukti"
    )
    proven_articles: list[CitedArticle] | None = Field(
        default=None,
        description="Pasal-pasal yang terbukti (termasuk juncto) secara terstruktur",
    )
    ruling_contents: list[str] | None = Field(
        default=None, description="Isi lengkap amar putusan"
    )
    sentences: VerdictSentences | None = Field(
        default=None, description="Detail hukuman yang dijatuhkan"
    )


# =============================================================================
# Nested Models - State Loss
# =============================================================================


class PerpetratorProceeds(BaseModel):
    """Individual perpetrator's corruption proceeds."""

    name: str | None = Field(default=None, description="Nama pelaku")
    amount: float | None = Field(
        default=None, description="Jumlah uang yang diperoleh (dalam Rupiah)"
    )
    role: str | None = Field(
        default=None, description="Jabatan/peran pelaku dalam kasus"
    )


class StateLoss(BaseModel):
    """State loss (kerugian negara) information."""

    auditor: str | None = Field(
        default=None,
        description="Instansi yang mengaudit kerugian negara (BPK/BPKP/Inspektorat)",
    )
    audit_report_number: str | None = Field(
        default=None, description="Nomor laporan hasil audit"
    )
    audit_report_date: str | None = Field(
        default=None, description="Tanggal laporan audit (Format: YYYY-MM-DD)"
    )
    indicted_amount: float | None = Field(
        default=None, description="Nilai kerugian negara yang didakwakan (dalam Rupiah)"
    )
    proven_amount: float | None = Field(
        default=None,
        description="Nilai kerugian negara yang dinyatakan terbukti (dalam Rupiah)",
    )
    returned_amount: float | None = Field(
        default=None,
        description="Jumlah kerugian negara yang sudah dikembalikan (dalam Rupiah)",
    )
    remaining_due: float | None = Field(
        default=None, description="Sisa yang wajib dikembalikan (dalam Rupiah)"
    )
    currency: str | None = Field(
        default="IDR", description="Mata uang (default: IDR)"
    )
    perpetrators_proceeds: list[PerpetratorProceeds] | None = Field(
        default=None,
        description="Rincian uang yang diperoleh masing-masing pelaku",
    )


# =============================================================================
# Nested Models - Related Case
# =============================================================================


class RelatedCase(BaseModel):
    """Related case information."""

    defendant_name: str | None = Field(
        default=None, description="Nama terdakwa dalam perkara terkait"
    )
    case_number: str | None = Field(
        default=None, description="Nomor perkara terkait"
    )
    status: str | None = Field(
        default=None,
        description="Status perkara terkait (separate_prosecution/splitsing/joined)",
    )
    relationship: str | None = Field(
        default=None,
        description="Hubungan dengan perkara utama (turut serta/membantu/menyuruh melakukan)",
    )


# =============================================================================
# Nested Models - Case Metadata
# =============================================================================


class CaseMetadata(BaseModel):
    """Additional case metadata."""

    crime_category: str | None = Field(
        default=None,
        description="Kategori tindak pidana (Korupsi/Penggelapan/Pencucian Uang/dll)",
    )
    crime_subcategory: str | None = Field(
        default=None,
        description="Subkategori tindak pidana (Penyalahgunaan Wewenang/Suap/Gratifikasi/dll)",
    )
    institution_involved: str | None = Field(
        default=None,
        description="Instansi/lembaga yang terlibat dalam kasus",
    )
    related_cases: list[RelatedCase] | None = Field(
        default=None, description="Perkara-perkara yang terkait"
    )


# =============================================================================
# Nested Models - Legal Facts (Categorized)
# =============================================================================


class CategorizedLegalFacts(BaseModel):
    """Categorized legal facts."""

    organizational_structure: list[str] | None = Field(
        default=None,
        description="Fakta tentang struktur organisasi yang terlibat",
    )
    standard_procedures: list[str] | None = Field(
        default=None, description="Fakta tentang prosedur standar yang seharusnya dijalankan"
    )
    violations: list[str] | None = Field(
        default=None, description="Fakta tentang pelanggaran yang dilakukan"
    )
    financial_irregularities: list[str] | None = Field(
        default=None, description="Fakta tentang penyimpangan keuangan"
    )
    witness_testimonies: list[str] | None = Field(
        default=None, description="Keterangan saksi-saksi"
    )
    documentary_evidence: list[str] | None = Field(
        default=None, description="Bukti-bukti dokumen"
    )
    other_facts: list[str] | None = Field(
        default=None, description="Fakta hukum lainnya yang tidak termasuk kategori di atas"
    )


# =============================================================================
# Nested Models - Judicial Considerations
# =============================================================================


class JudicialConsiderations(BaseModel):
    """Judge's considerations in the verdict."""

    aggravating_factors: list[str] | None = Field(
        default=None, description="Hal-hal yang memberatkan hukuman"
    )
    mitigating_factors: list[str] | None = Field(
        default=None, description="Hal-hal yang meringankan hukuman"
    )


# =============================================================================
# Nested Models - Detention History
# =============================================================================


class DetentionPeriod(BaseModel):
    """Single detention period entry."""

    stage: str | None = Field(
        default=None,
        description="Tahapan penahanan (Penyidik/Penuntut Umum/Hakim/Perpanjangan)",
    )
    start_date: str | None = Field(
        default=None, description="Tanggal mulai penahanan (Format: YYYY-MM-DD)"
    )
    end_date: str | None = Field(
        default=None, description="Tanggal berakhir penahanan (Format: YYYY-MM-DD)"
    )
    duration_days: int | None = Field(
        default=None, description="Durasi penahanan dalam hari"
    )
    location: str | None = Field(
        default=None, description="Lokasi penahanan (Rutan/Lapas/Tahanan Kota)"
    )


# =============================================================================
# Nested Models - Lower Court Decision (for Appeal Cases)
# =============================================================================


class LowerCourtSentence(BaseModel):
    """Sentence details from lower court."""

    imprisonment: str | None = Field(
        default=None, description="Hukuman penjara dari pengadilan tingkat pertama"
    )
    fine: str | None = Field(
        default=None, description="Denda dari pengadilan tingkat pertama"
    )
    restitution: str | None = Field(
        default=None, description="Uang pengganti dari pengadilan tingkat pertama"
    )


class LowerCourtDecision(BaseModel):
    """Information about the lower court's decision (for appeal cases)."""

    court_name: str | None = Field(
        default=None, description="Nama pengadilan tingkat pertama"
    )
    verdict_number: str | None = Field(
        default=None, description="Nomor putusan pengadilan tingkat pertama"
    )
    verdict_date: str | None = Field(
        default=None,
        description="Tanggal putusan pengadilan tingkat pertama (Format: YYYY-MM-DD)",
    )
    primary_charge_ruling: str | None = Field(
        default=None,
        description="Putusan dakwaan primer (Terbukti/Tidak Terbukti/Bebas)",
    )
    subsidiary_charge_ruling: str | None = Field(
        default=None,
        description="Putusan dakwaan subsidiair (Terbukti/Tidak Terbukti)",
    )
    sentence: LowerCourtSentence | None = Field(
        default=None, description="Detail hukuman dari pengadilan tingkat pertama"
    )


# =============================================================================
# Nested Models - Appeal Process
# =============================================================================


class AppealProcess(BaseModel):
    """Information about the appeal process."""

    applicant: str | None = Field(
        default=None,
        description="Pihak yang mengajukan banding (Penuntut Umum/Terdakwa/Keduanya)",
    )
    request_date: str | None = Field(
        default=None, description="Tanggal permohonan banding (Format: YYYY-MM-DD)"
    )
    registration_date: str | None = Field(
        default=None, description="Tanggal registrasi banding (Format: YYYY-MM-DD)"
    )
    notification_to_defendant: str | None = Field(
        default=None,
        description="Tanggal pemberitahuan kepada terdakwa (Format: YYYY-MM-DD)",
    )
    notification_to_prosecutor: str | None = Field(
        default=None,
        description="Tanggal pemberitahuan kepada JPU (Format: YYYY-MM-DD)",
    )
    memorandum_filed: bool | None = Field(
        default=None, description="Apakah memori banding diajukan"
    )
    memorandum_date: str | None = Field(
        default=None, description="Tanggal pengajuan memori banding (Format: YYYY-MM-DD)"
    )
    contra_memorandum_filed: bool | None = Field(
        default=None, description="Apakah kontra memori banding diajukan"
    )
    contra_memorandum_date: str | None = Field(
        default=None,
        description="Tanggal pengajuan kontra memori banding (Format: YYYY-MM-DD)",
    )
    judge_notes: str | None = Field(
        default=None, description="Catatan hakim terkait proses banding"
    )


# =============================================================================
# Nested Models - Evidence Inventory
# =============================================================================


class EvidenceItem(BaseModel):
    """Single evidence item."""

    item: str | None = Field(
        default=None, description="Deskripsi barang bukti"
    )
    recipient: str | None = Field(
        default=None, description="Penerima barang bukti (jika dikembalikan)"
    )
    condition: str | None = Field(
        default=None, description="Kondisi/status barang bukti"
    )
    status: str | None = Field(
        default=None, description="Status disposisi barang bukti"
    )


class EvidenceInventory(BaseModel):
    """Categorized inventory of evidence items."""

    returned_to_defendant: list[EvidenceItem] | None = Field(
        default=None, description="Barang bukti yang dikembalikan kepada terdakwa"
    )
    returned_to_third_party: list[EvidenceItem] | None = Field(
        default=None, description="Barang bukti yang dikembalikan kepada pihak ketiga"
    )
    confiscated_for_state: list[EvidenceItem] | None = Field(
        default=None, description="Barang bukti yang dirampas untuk negara"
    )
    destroyed: list[EvidenceItem] | None = Field(
        default=None, description="Barang bukti yang dimusnahkan"
    )
    attached_to_case_file: list[EvidenceItem] | None = Field(
        default=None, description="Barang bukti yang tetap terlampir dalam berkas"
    )
    used_in_other_case: list[EvidenceItem] | None = Field(
        default=None, description="Barang bukti yang digunakan dalam perkara lain"
    )


# =============================================================================
# Nested Models - Additional Case Data
# =============================================================================


class AdditionalCaseData(BaseModel):
    """Additional case data for complex cases (appeals, multi-stage proceedings)."""

    detention_history: list[DetentionPeriod] | None = Field(
        default=None, description="Riwayat penahanan terdakwa dari awal hingga putusan"
    )
    lower_court_decision: LowerCourtDecision | None = Field(
        default=None,
        description="Putusan pengadilan tingkat pertama (untuk perkara banding)",
    )
    appeal_process: AppealProcess | None = Field(
        default=None, description="Informasi proses banding"
    )
    evidence_inventory: EvidenceInventory | None = Field(
        default=None, description="Inventarisasi dan disposisi barang bukti"
    )


# =============================================================================
# Nested Models - Defense Strategy
# =============================================================================


class ExceptionArgument(BaseModel):
    """Exception argument type and details."""

    type: str | None = Field(
        default=None,
        description="Jenis argumen eksepsi (competence/indictment_validity/procedure)",
    )
    description: str | None = Field(
        default=None, description="Deskripsi argumen eksepsi"
    )
    details: str | None = Field(
        default=None, description="Detail argumen eksepsi"
    )


class ProsecutorExceptionResponse(BaseModel):
    """Prosecutor's response to exception."""

    date: str | None = Field(
        default=None, description="Tanggal tanggapan JPU (Format: YYYY-MM-DD)"
    )
    summary: str | None = Field(
        default=None, description="Ringkasan tanggapan Penuntut Umum atas Eksepsi"
    )


class InterlocutoryRuling(BaseModel):
    """Court's interlocutory ruling on exception."""

    date: str | None = Field(
        default=None, description="Tanggal putusan sela (Format: YYYY-MM-DD)"
    )
    decision: str | None = Field(
        default=None, description="Keputusan putusan sela (Diterima/Ditolak)"
    )
    reasoning: str | None = Field(
        default=None, description="Alasan/pertimbangan putusan sela"
    )


class DefenseException(BaseModel):
    """Defense exception (eksepsi) information."""

    filed: bool | None = Field(
        default=None, description="Apakah eksepsi diajukan"
    )
    date: str | None = Field(
        default=None, description="Tanggal pengajuan eksepsi (Format: YYYY-MM-DD)"
    )
    summary: str | None = Field(
        default=None, description="Ringkasan eksepsi yang diajukan"
    )
    primary_arguments: list[ExceptionArgument] | None = Field(
        default=None, description="Argumen utama dalam eksepsi"
    )
    prosecutor_response: ProsecutorExceptionResponse | None = Field(
        default=None, description="Tanggapan Penuntut Umum atas eksepsi"
    )
    court_interlocutory_ruling: InterlocutoryRuling | None = Field(
        default=None, description="Putusan sela pengadilan atas eksepsi"
    )


class PersonalPlea(BaseModel):
    """Personal plea from defendant."""

    filed: bool | None = Field(
        default=False, description="Apakah pembelaan pribadi diajukan"
    )
    summary: str | None = Field(
        default=None,
        description="Ringkasan pembelaan pribadi terdakwa (biasanya memohon keringanan)",
    )


class PleaArgument(BaseModel):
    """Key argument in legal counsel's plea."""

    point: str | None = Field(
        default=None,
        description="Poin argumen (unsur_melawan_hukum/unsur_memperkaya_diri/procedural_flaws)",
    )
    argument: str | None = Field(
        default=None, description="Isi argumen"
    )


class LegalCounselPlea(BaseModel):
    """Legal counsel's plea (pledoi)."""

    filed: bool | None = Field(
        default=False, description="Apakah pledoi pengacara diajukan"
    )
    summary: str | None = Field(
        default=None, description="Ringkasan pembelaan yuridis dari pengacara"
    )
    key_arguments: list[PleaArgument] | None = Field(
        default=None, description="Argumen utama dalam pledoi"
    )
    specific_requests: list[str] | None = Field(
        default=None,
        description="Permintaan spesifik (Vrijspraak/Onslag/Keringanan/Pengembalian aset)",
    )


class DefensePlea(BaseModel):
    """Defense plea (pledoi) information."""

    date: str | None = Field(
        default=None, description="Tanggal pengajuan pledoi (Format: YYYY-MM-DD)"
    )
    submitted_by: list[str] | None = Field(
        default=None, description="Pihak yang mengajukan pledoi"
    )
    personal_plea: PersonalPlea | None = Field(
        default=None, description="Pembelaan pribadi dari terdakwa"
    )
    legal_counsel_plea: LegalCounselPlea | None = Field(
        default=None, description="Pembelaan yuridis dari pengacara"
    )


class Replik(BaseModel):
    """Prosecutor's response to plea (replik)."""

    date: str | None = Field(
        default=None, description="Tanggal replik (Format: YYYY-MM-DD)"
    )
    summary: str | None = Field(
        default=None, description="Ringkasan tanggapan Jaksa atas Pledoi"
    )


class Duplik(BaseModel):
    """Defense response to replik (duplik)."""

    date: str | None = Field(
        default=None, description="Tanggal duplik (Format: YYYY-MM-DD)"
    )
    summary: str | None = Field(
        default=None, description="Ringkasan tanggapan balik Pengacara atas Replik"
    )


class Rejoinders(BaseModel):
    """Rejoinder information (replik and duplik)."""

    replik: Replik | None = Field(
        default=None, description="Tanggapan Jaksa atas Pledoi"
    )
    duplik: Duplik | None = Field(
        default=None, description="Tanggapan balik Pengacara atas Replik"
    )


class DefenseStrategy(BaseModel):
    """Complete defense strategy information."""

    exception: DefenseException | None = Field(
        default=None, description="Informasi eksepsi yang diajukan"
    )
    plea: DefensePlea | None = Field(
        default=None, description="Informasi pledoi/pembelaan"
    )
    rejoinders: Rejoinders | None = Field(
        default=None, description="Informasi replik dan duplik"
    )


# =============================================================================
# Nested Models - Legal Entity Analysis
# =============================================================================


class EntityRegistryItem(BaseModel):
    """Individual legal entity involved in the case."""

    id: str | None = Field(
        default=None, description="Unique identifier for the entity (e.g., ENT_01)"
    )
    name: str | None = Field(
        default=None, description="Nama entitas/badan hukum"
    )
    legal_form: str | None = Field(
        default=None,
        description="Bentuk hukum (Instansi Pemerintah/PT/CV/Yayasan/dll)",
    )
    sector: str | None = Field(
        default=None,
        description="Sektor entitas (Pendidikan/Swasta/Pengawasan/dll)",
    )
    role_in_case: str | None = Field(
        default=None,
        description="Peran entitas dalam kasus (Locus Delicti/Korban/Vendor/Auditor/dll)",
    )
    address: str | None = Field(
        default=None, description="Alamat entitas"
    )


class AffiliationMapItem(BaseModel):
    """Person-to-entity affiliation mapping."""

    person_name: str | None = Field(
        default=None, description="Nama orang yang berafiliasi"
    )
    related_entity_id: str | None = Field(
        default=None,
        description="ID entitas terkait (merujuk ke entity_registry.id)",
    )
    position: str | None = Field(
        default=None,
        description="Jabatan/posisi dalam entitas (Direktur/Kepala/Auditor/dll)",
    )
    nature_of_relationship: str | None = Field(
        default=None,
        description="Sifat hubungan (Struktural/Kepemilikan/Penugasan Resmi/dll)",
    )


class LegalEntityAnalysis(BaseModel):
    """Analysis of legal entities involved in the case."""

    entity_registry: list[EntityRegistryItem] | None = Field(
        default=None,
        description="Daftar entitas/badan hukum yang terlibat dalam kasus",
    )
    affiliations_map: list[AffiliationMapItem] | None = Field(
        default=None,
        description="Peta afiliasi antara orang dengan entitas",
    )


# =============================================================================
# Main ExtractionResult Model (Restructured)
# =============================================================================


class ExtractionResult(BaseModel):
    """
    Structured output model for LLM extraction from court decision documents.

    This model uses nested objects for better organization and data structure.
    """

    # Defendant Information (Nested)
    defendant: DefendantInfo | None = Field(
        default=None, description="Informasi lengkap tentang terdakwa"
    )

    # Legal Counsel (Nested Array)
    legal_counsels: list[LegalCounsel] | None = Field(
        default=None, description="Daftar penasihat hukum terdakwa"
    )

    # Court Information (Nested)
    court: CourtInfo | None = Field(
        default=None, description="Informasi pengadilan dan perkara"
    )

    # Court Personnel (Nested)
    court_personnel: CourtPersonnel | None = Field(
        default=None, description="Pihak-pihak pengadilan yang terlibat"
    )

    # Indictment (Nested)
    indictment: Indictment | None = Field(
        default=None, description="Informasi dakwaan"
    )

    # Prosecution Demand (Nested)
    prosecution_demand: ProsecutionDemand | None = Field(
        default=None, description="Informasi tuntutan Jaksa Penuntut Umum"
    )

    # Defense Strategy (Nested)
    defense_strategy: DefenseStrategy | None = Field(
        default=None,
        description="Strategi pembelaan terdakwa (eksepsi, pledoi, replik/duplik)",
    )

    # Legal Facts (Categorized)
    legal_facts: CategorizedLegalFacts | None = Field(
        default=None, description="Fakta-fakta hukum yang terungkap di persidangan"
    )

    # Judicial Considerations (Nested)
    judicial_considerations: JudicialConsiderations | None = Field(
        default=None, description="Pertimbangan hakim dalam putusan"
    )

    # Verdict (Nested)
    verdict: Verdict | None = Field(
        default=None, description="Informasi putusan pengadilan"
    )

    # State Loss (Nested)
    state_loss: StateLoss | None = Field(
        default=None, description="Informasi kerugian negara"
    )

    # Case Metadata (Nested)
    case_metadata: CaseMetadata | None = Field(
        default=None, description="Metadata tambahan tentang perkara"
    )

    # Legal Entity Analysis (Nested)
    legal_entity_analysis: LegalEntityAnalysis | None = Field(
        default=None,
        description="Analisis entitas/badan hukum yang terlibat dalam kasus",
    )

    # Additional Case Data (Nested) - for appeal/complex cases
    additional_case_data: AdditionalCaseData | None = Field(
        default=None,
        description="Data tambahan untuk perkara kompleks (banding, kasasi, multi-tahap)",
    )

    # Extraction confidence
    extraction_confidence: float | None = Field(
        default=None,
        description="Overall confidence score (0.0-1.0) indicating how confident "
        "the model is about the accuracy of all extracted information. "
        "1.0 means very confident, 0.0 means very uncertain.",
    )


class ExtractionStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class LLMExtraction(SQLModel, table=True):
    """SQLModel for llm_extractions table."""

    __tablename__ = "llm_extractions"

    id: str = SQLField(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    extraction_id: str = SQLField(unique=True, index=True)
    extraction_result: dict | None = SQLField(
        default=None, sa_column=Column(JSONB, nullable=True)
    )
    summary_en: str | None = SQLField(default=None)
    summary_id: str | None = SQLField(default=None)
    extraction_confidence: float | None = SQLField(default=None)
    status: str = SQLField(default=ExtractionStatus.PENDING.value)
    created_at: datetime = SQLField(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
    )
    updated_at: datetime = SQLField(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
    )


# JSON Schema description for the extraction output
EXTRACTION_JSON_SCHEMA = """
The output must be a valid JSON object with this structure:

{
  "defendant": {
    "name": "string or null",
    "alias": "string or null",
    "place_of_birth": "string or null",
    "date_of_birth": "YYYY-MM-DD or null",
    "age": "integer or null",
    "gender": "Laki-laki/Perempuan or null",
    "citizenship": "string or null",
    "address": {
      "street": "string or null",
      "rt_rw": "string or null",
      "kelurahan": "string or null",
      "kecamatan": "string or null",
      "city": "string or null",
      "province": "string or null",
      "full_address": "string or null"
    },
    "religion": "string or null",
    "occupation": "string or null",
    "education": "string or null"
  },
  "legal_counsels": [
    {
      "name": "string or null",
      "office_name": "string or null",
      "office_address": "string or null"
    }
  ],
  "court": {
    "case_register_number": "string or null",
    "verdict_number": "string or null",
    "court_name": "string or null",
    "court_level": "Pengadilan Negeri/Pengadilan Tinggi/Mahkamah Agung or null",
    "province": "string or null",
    "city": "string or null"
  },
  "court_personnel": {
    "judges": [{"name": "string", "role": "Ketua Majelis/Hakim Anggota"}],
    "prosecutors": ["string"],
    "court_clerks": ["string"]
  },
  "indictment": {
    "type": "Tunggal/Alternatif/Subsidiair/Kumulatif/Campuran or null",
    "chronology": "string or null",
    "crime_location": "string or null",
    "crime_period": {
      "start_date": "YYYY-MM-DD or null",
      "end_date": "YYYY-MM-DD or null",
      "description": "string or null"
    },
    "cited_articles": [
      {
        "article": "string",
        "law_name": "string",
        "law_number": "string or null",
        "law_year": "integer or null",
        "full_citation": "string"
      }
    ],
    "defense_exception_status": "Ditolak/Diterima/Tidak Ada or null"
  },
  "prosecution_demand": {
    "date": "YYYY-MM-DD or null",
    "articles": [{"article": "string", "law_name": "string", "full_citation": "string"}],
    "content": "string or null",
    "prison_sentence_months": "float or null",
    "prison_sentence_description": "string or null",
    "fine_amount": "float or null",
    "fine_subsidiary_confinement_months": "integer or null",
    "restitution_amount": "float or null",
    "restitution_subsidiary_type": "kurungan/penjara or null",
    "restitution_subsidiary_duration_months": "integer or null"
  },
  "defense_strategy": {
    "exception": {
      "filed": "boolean or null",
      "date": "YYYY-MM-DD or null",
      "summary": "string or null",
      "primary_arguments": [
        {
          "type": "competence/indictment_validity/procedure or null",
          "description": "string or null",
          "details": "string or null"
        }
      ],
      "prosecutor_response": {
        "date": "YYYY-MM-DD or null",
        "summary": "string or null"
      },
      "court_interlocutory_ruling": {
        "date": "YYYY-MM-DD or null",
        "decision": "Diterima/Ditolak or null",
        "reasoning": "string or null"
      }
    },
    "plea": {
      "date": "YYYY-MM-DD or null",
      "submitted_by": ["string"],
      "personal_plea": {
        "filed": "boolean or null",
        "summary": "string or null"
      },
      "legal_counsel_plea": {
        "filed": "boolean or null",
        "summary": "string or null",
        "key_arguments": [
          {
            "point": "unsur_melawan_hukum/unsur_memperkaya_diri/procedural_flaws or null",
            "argument": "string or null"
          }
        ],
        "specific_requests": ["Vrijspraak/Onslag/Keringanan/Pengembalian aset"]
      }
    },
    "rejoinders": {
      "replik": {
        "date": "YYYY-MM-DD or null",
        "summary": "string or null"
      },
      "duplik": {
        "date": "YYYY-MM-DD or null",
        "summary": "string or null"
      }
    }
  },
  "legal_facts": {
    "organizational_structure": ["string"],
    "standard_procedures": ["string"],
    "violations": ["string"],
    "financial_irregularities": ["string"],
    "witness_testimonies": ["string"],
    "documentary_evidence": ["string"],
    "other_facts": ["string"]
  },
  "judicial_considerations": {
    "aggravating_factors": ["string"],
    "mitigating_factors": ["string"]
  },
  "verdict": {
    "number": "string or null",
    "date": "YYYY-MM-DD or null",
    "day": "string or null",
    "year": "integer or null",
    "result": "guilty/partially_guilty/not_guilty/acquitted or null",
    "primary_charge_proven": "boolean or null",
    "subsidiary_charge_proven": "boolean or null",
    "proven_articles": [
      {
        "article": "Pasal X Ayat (Y)",
        "law_name": "nama undang-undang",
        "law_number": "string or null",
        "law_year": "integer or null",
        "full_citation": "kutipan lengkap pasal jo. juncto"
      }
    ],
    "ruling_contents": ["string"],
    "sentences": {
      "imprisonment": {"duration_months": "integer or null", "description": "string or null"},
      "fine": {"amount": "float or null", "subsidiary_confinement_months": "integer or null"},
      "restitution": {
        "amount": "float or null",
        "already_paid": "float or null",
        "remaining": "float or null",
        "subsidiary_type": "kurungan/penjara or null",
        "subsidiary_duration_months": "integer or null"
      }
    }
  },
  "state_loss": {
    "auditor": "string or null",
    "audit_report_number": "string or null",
    "audit_report_date": "YYYY-MM-DD or null",
    "indicted_amount": "float or null",
    "proven_amount": "float or null",
    "returned_amount": "float or null",
    "remaining_due": "float or null",
    "currency": "IDR",
    "perpetrators_proceeds": [{"name": "string", "amount": "float", "role": "string"}]
  },
  "case_metadata": {
    "crime_category": "string or null",
    "crime_subcategory": "string or null",
    "institution_involved": "string or null",
    "related_cases": [
      {"defendant_name": "string", "case_number": "string or null", "status": "string", "relationship": "string"}
    ]
  },
  "legal_entity_analysis": {
    "entity_registry": [
      {
        "id": "string (e.g., ENT_01)",
        "name": "string or null",
        "legal_form": "Instansi Pemerintah/PT/CV/Yayasan or null",
        "sector": "string or null",
        "role_in_case": "string or null",
        "address": "string or null"
      }
    ],
    "affiliations_map": [
      {
        "person_name": "string or null",
        "related_entity_id": "string (reference to entity_registry.id)",
        "position": "string or null",
        "nature_of_relationship": "Struktural/Kepemilikan/Penugasan Resmi or null"
      }
    ]
  },
  "additional_case_data": {
    "detention_history": [
      {"stage": "string", "start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD", "duration_days": "integer", "location": "string"}
    ],
    "lower_court_decision": {
      "court_name": "string or null",
      "verdict_number": "string or null",
      "verdict_date": "YYYY-MM-DD or null",
      "primary_charge_ruling": "string or null",
      "subsidiary_charge_ruling": "string or null",
      "sentence": {"imprisonment": "string", "fine": "string", "restitution": "string"}
    },
    "appeal_process": {
      "applicant": "string or null",
      "request_date": "YYYY-MM-DD or null",
      "registration_date": "YYYY-MM-DD or null",
      "notification_to_defendant": "YYYY-MM-DD or null",
      "notification_to_prosecutor": "YYYY-MM-DD or null",
      "memorandum_filed": "boolean or null",
      "memorandum_date": "YYYY-MM-DD or null",
      "contra_memorandum_filed": "boolean or null",
      "contra_memorandum_date": "YYYY-MM-DD or null",
      "judge_notes": "string or null"
    },
    "evidence_inventory": {
      "returned_to_defendant": [{"item": "string", "recipient": "string", "condition": "string", "status": "string"}],
      "returned_to_third_party": [{"item": "string", "recipient": "string", "condition": "string", "status": "string"}],
      "confiscated_for_state": [{"item": "string", "recipient": "string", "condition": "string", "status": "string"}],
      "destroyed": [{"item": "string", "recipient": "string", "condition": "string", "status": "string"}],
      "attached_to_case_file": [{"item": "string", "recipient": "string", "condition": "string", "status": "string"}],
      "used_in_other_case": [{"item": "string", "recipient": "string", "condition": "string", "status": "string"}]
    }
  },
  "extraction_confidence": "float between 0.0 and 1.0"
}
"""

# System prompt for extraction
EXTRACTION_SYSTEM_PROMPT = f"""You are a professional legal expert specialized in analyzing Indonesian Supreme Court (Mahkamah Agung) decision documents.

Your task is to extract structured information from court decision documents. You will receive:
1. The current extraction result (may be empty or partially filled)
2. A chunk of text from the court decision document

You must:
1. Carefully read and understand the provided text chunk
2. Extract any relevant information that matches the required fields
3. Update the extraction result with newly found information
4. Preserve existing information unless you find more accurate/complete data
5. Return the updated extraction result as valid JSON

Important guidelines:
- Only extract information that is explicitly stated in the document
- Use null for fields where information is not found
- Dates should be in YYYY-MM-DD format
- Monetary values should be numbers without currency symbols
- Prison sentences in months should be converted (e.g., "1 tahun 6 bulan" = 18)
- Be careful to distinguish between prosecution demands (tuntutan) and final verdict (putusan)
- ALWAYS provide an extraction_confidence score (0.0-1.0) indicating your overall confidence
- When encountering tables in the document, preserve them in markdown table format
- Return ONLY valid JSON, no markdown code blocks or explanations

{EXTRACTION_JSON_SCHEMA}
"""

EXTRACTION_PROMPT = """# CURRENT EXTRACTION RESULT
This is the current state of extracted information (update with new findings):

{current_extraction}

# DOCUMENT CHUNK
This is a chunk from the court decision document (chunk {chunk_number} of {total_chunks}):

{chunk_content}

# INSTRUCTIONS
1. Read the document chunk carefully
2. Extract any relevant information for the structured fields
3. Update the extraction result with new information found
4. Preserve existing data unless you find more accurate information
5. Provide an extraction_confidence score (0.0-1.0) based on document quality and extraction certainty
6. Return the complete updated extraction result
"""

SUMMARY_SYSTEM_PROMPT = """You are a professional legal expert who specializes in summarizing Indonesian Supreme Court decision documents.

Your task is to create a concise, comprehensive summary of a court case based on the extracted information.
"""

SUMMARY_PROMPT_ID = """Buat ringkasan putusan pengadilan berdasarkan data JSON berikut:

{extraction_result}

Format output (mulai langsung dengan heading pertama, TANPA kalimat pembuka):

## Identitas Terdakwa dan Perkara
- Nama: [dari defendant.name]
- TTL: [dari defendant.place_of_birth, defendant.date_of_birth]
- Alamat: [dari defendant.address]
- Pekerjaan: [dari defendant.occupation]
- No. Perkara: [dari court.case_register_number]

## Kronologi Kasus
[dari indictment.chronology]

## Dakwaan
[dari indictment.type dan indictment.cited_articles]

## Tuntutan Jaksa Penuntut Umum
[dari prosecution_demand]

## Fakta Hukum
[dari legal_facts]

## Pertimbangan Hakim
[dari judicial_considerations]

## Putusan
[dari verdict]

PENTING: Mulai langsung dengan "## Identitas Terdakwa dan Perkara", jangan ada teks sebelumnya.
"""

SUMMARY_PROMPT_EN = """Create a court decision summary based on this JSON data:

{extraction_result}

Output format (start directly with first heading, NO introductory text):

## Defendant Identity and Case Information
- Name: [from defendant.name]
- DOB: [from defendant.place_of_birth, defendant.date_of_birth]
- Address: [from defendant.address]
- Occupation: [from defendant.occupation]
- Case Number: [from court.case_register_number]

## Case Chronology
[from indictment.chronology]

## Indictment
[from indictment.type and indictment.cited_articles]

## Prosecutor's Demands
[from prosecution_demand]

## Legal Facts
[from legal_facts]

## Judge's Considerations
[from judicial_considerations]

## Verdict
[from verdict]

IMPORTANT: Start directly with "## Defendant Identity and Case Information", no text before it.
"""


def chunk_document(doc_content: dict[int, str], chunk_size: int = CHUNK_SIZE) -> list[str]:
    """
    Chunk document content into chunks of specified page size.

    Args:
        doc_content: Dictionary mapping page numbers to text content
        chunk_size: Number of pages per chunk (default: 100)

    Returns:
        List of text chunks
    """
    if not doc_content:
        return []

    # Sort pages by page number
    sorted_pages = sorted(doc_content.items(), key=lambda x: x[0])

    chunks = []
    current_chunk = []
    current_chunk_pages = 0

    for page_num, content in sorted_pages:
        current_chunk.append(content)
        current_chunk_pages += 1

        if current_chunk_pages >= chunk_size:
            chunks.append("\n\n".join(current_chunk))
            current_chunk = []
            current_chunk_pages = 0

    # Add remaining pages as final chunk
    if current_chunk:
        chunks.append("\n\n".join(current_chunk))

    logger.info(f"Document chunked into {len(chunks)} chunks of {chunk_size} pages each")
    return chunks


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def extract_from_chunk(
    chunk_content: str,
    current_extraction: dict[str, Any],
    chunk_number: int,
    total_chunks: int,
) -> ExtractionResult:
    """
    Extract information from a single document chunk using LLM.

    Args:
        chunk_content: Text content of the chunk
        current_extraction: Current extraction result to update
        chunk_number: Current chunk number (1-indexed)
        total_chunks: Total number of chunks

    Returns:
        Updated ExtractionResult
    """
    logger.debug(f"Extracting from chunk {chunk_number}/{total_chunks}")

    messages = [
        {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": EXTRACTION_PROMPT.format(
                current_extraction=json.dumps(current_extraction, indent=2, ensure_ascii=False),
                chunk_number=chunk_number,
                total_chunks=total_chunks,
                chunk_content=chunk_content,
            ),
        },
    ]

    response = await acompletion(
        model=MODEL,
        messages=messages,
        response_format={"type": "json_object"},
    )

    raw_content = response.choices[0].message.content
    logger.debug(f"Raw LLM response for chunk {chunk_number}: {raw_content[:500]}...")

    # Clean up response - remove markdown code blocks if present
    cleaned_content = raw_content.strip()
    if cleaned_content.startswith("```json"):
        cleaned_content = cleaned_content[7:]
    elif cleaned_content.startswith("```"):
        cleaned_content = cleaned_content[3:]
    if cleaned_content.endswith("```"):
        cleaned_content = cleaned_content[:-3]
    cleaned_content = cleaned_content.strip()

    parsed_json = None
    try:
        parsed_json = json.loads(cleaned_content)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from chunk {chunk_number}: {e}")
        logger.error(f"Cleaned content: {cleaned_content[:500]}")
        logger.error(f"Traceback:\n{traceback.format_exc()}")
        raise

    try:
        result = ExtractionResult(**parsed_json)

        # Check if result is mostly empty
        non_null_fields = sum(
            1 for v in result.model_dump().values() if v is not None
        )
        logger.info(
            f"Chunk {chunk_number}: extracted {non_null_fields} non-null fields"
        )

        if non_null_fields <= 1:  # Only extraction_confidence or nothing
            logger.warning(
                f"Chunk {chunk_number} extraction mostly empty. "
                f"Raw response preview: {raw_content[:200]}"
            )
    except Exception as e:
        logger.error(f"Failed to validate extraction from chunk {chunk_number}: {e}")
        logger.error(
            f"Parsed JSON keys: {list(parsed_json.keys()) if parsed_json else 'N/A'}"
        )
        logger.error(f"Traceback:\n{traceback.format_exc()}")
        raise

    logger.debug(f"Successfully extracted from chunk {chunk_number}")
    return result


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def generate_summary_id(extraction_result: dict[str, Any]) -> str:
    """Generate summary in Indonesian from extraction result."""
    messages = [
        {"role": "system", "content": SUMMARY_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": SUMMARY_PROMPT_ID.format(
                extraction_result=json.dumps(extraction_result, indent=2, ensure_ascii=False)
            ),
        },
    ]

    response = await acompletion(
        model=MODEL,
        messages=messages,
    )

    return response.choices[0].message.content


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def generate_summary_en(extraction_result: dict[str, Any]) -> str:
    """Generate summary in English from extraction result."""
    messages = [
        {"role": "system", "content": SUMMARY_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": SUMMARY_PROMPT_EN.format(
                extraction_result=json.dumps(extraction_result, indent=2, ensure_ascii=False)
            ),
        },
    ]

    response = await acompletion(
        model=MODEL,
        messages=messages,
    )

    return response.choices[0].message.content


async def process_document_extraction(
    decision_number: str,
    doc_content: dict[int, str],
) -> tuple[ExtractionResult, str, str]:
    """
    Process document through the extraction pipeline.

    Args:
        decision_number: The court decision number
        doc_content: Dictionary mapping page numbers to text content

    Returns:
        Tuple of (ExtractionResult, summary_id, summary_en)
    """
    logger.info(f"Starting extraction pipeline for decision: {decision_number}")

    # Validate doc_content
    if not doc_content:
        logger.error(f"Empty doc_content for decision: {decision_number}")
        raise ValueError(f"No document content to extract for {decision_number}")

    total_chars = sum(len(content) for content in doc_content.values())
    logger.info(
        f"Document has {len(doc_content)} pages, {total_chars} total characters"
    )

    # Step 1: Chunk the document
    chunks = chunk_document(doc_content)
    total_chunks = len(chunks)
    logger.info(f"Document split into {total_chunks} chunks")

    if total_chunks == 0:
        logger.error(f"No chunks created for decision: {decision_number}")
        raise ValueError(f"Failed to create chunks for {decision_number}")

    # Step 2: Process each chunk iteratively
    current_extraction: dict[str, Any] = {}

    for i, chunk in enumerate(tqdm(chunks, desc=f"Processing {decision_number}")):
        chunk_number = i + 1
        chunk_length = len(chunk) if chunk else 0
        logger.info(
            f"Processing chunk {chunk_number}/{total_chunks} for {decision_number} "
            f"(chunk size: {chunk_length} chars)"
        )

        if chunk_length == 0:
            logger.warning(f"Skipping empty chunk {chunk_number}/{total_chunks}")
            continue

        try:
            result = await extract_from_chunk(
                chunk_content=chunk,
                current_extraction=current_extraction,
                chunk_number=chunk_number,
                total_chunks=total_chunks,
            )
            # Update current extraction with new results
            current_extraction = result.model_dump(exclude_none=True)
            logger.debug(f"Chunk {chunk_number} processed, fields extracted: {len(current_extraction)}")
        except Exception as e:
            logger.error(f"Error processing chunk {chunk_number}: {e}")
            logger.error(f"Full traceback:\n{traceback.format_exc()}")
            # Continue with next chunk even if one fails
            continue

    # Step 3: Generate summaries
    logger.info(f"Generating summaries for {decision_number}")

    summary_id = await generate_summary_id(current_extraction)
    summary_en = await generate_summary_en(current_extraction)

    logger.info(f"Extraction pipeline completed for {decision_number}")

    return ExtractionResult(**current_extraction), summary_id, summary_en


async def save_llm_extraction(
    db_engine: AsyncEngine,
    extraction_id: str,
    extraction_result: ExtractionResult,
    summary_en: str,
    summary_id: str,
    status: ExtractionStatus = ExtractionStatus.COMPLETED,
) -> LLMExtraction:
    """
    Save LLM extraction result to database.

    Args:
        db_engine: Async database engine
        extraction_id: ID of the source extraction
        extraction_result: The extraction result from LLM
        summary_en: English summary
        summary_id: Indonesian summary
        status: Extraction status

    Returns:
        The saved LLMExtraction record
    """
    logger.info(f"Saving LLM extraction for extraction_id: {extraction_id}")

    async_session = async_sessionmaker(bind=db_engine, class_=AsyncSession)

    async with async_session() as session:
        try:
            # Check if record already exists
            result = await session.execute(
                select(LLMExtraction).where(LLMExtraction.extraction_id == extraction_id)
            )
            existing = result.scalar_one_or_none()

            if existing:
                # Update existing record
                existing.extraction_result = extraction_result.model_dump()
                existing.summary_en = summary_en
                existing.summary_id = summary_id
                existing.extraction_confidence = extraction_result.extraction_confidence
                existing.status = status.value
                existing.updated_at = datetime.now(timezone.utc)
                session.add(existing)
                await session.commit()
                await session.refresh(existing)
                logger.info(f"Updated existing LLM extraction record: {existing.id}")
                return existing
            else:
                # Create new record
                llm_extraction = LLMExtraction(
                    extraction_id=extraction_id,
                    extraction_result=extraction_result.model_dump(),
                    summary_en=summary_en,
                    summary_id=summary_id,
                    extraction_confidence=extraction_result.extraction_confidence,
                    status=status.value,
                )
                session.add(llm_extraction)
                await session.commit()
                await session.refresh(llm_extraction)
                logger.info(f"Created new LLM extraction record: {llm_extraction.id}")
                return llm_extraction
        except Exception as e:
            logger.error(f"Error saving LLM extraction: {e}")
            await session.rollback()
            raise


async def update_llm_extraction_status(
    db_engine: AsyncEngine,
    extraction_id: str,
    status: ExtractionStatus,
) -> None:
    """Update the status of an LLM extraction record."""
    async_session = async_sessionmaker(bind=db_engine, class_=AsyncSession)

    async with async_session() as session:
        try:
            result = await session.execute(
                select(LLMExtraction).where(LLMExtraction.extraction_id == extraction_id)
            )
            existing = result.scalar_one_or_none()

            if existing:
                existing.status = status.value
                existing.updated_at = datetime.now(timezone.utc)
                session.add(existing)
                await session.commit()
                logger.debug(
                    f"Updated status to {status.value} for extraction_id: {extraction_id}"
                )
        except Exception as e:
            logger.error(f"Error updating LLM extraction status: {e}")
            await session.rollback()
            raise
