"""
Pydantic models for Indonesian Supreme Court document extraction.

This module contains all the data models used for structured extraction
from court decision documents.
"""

from enum import Enum

from pydantic import BaseModel, Field

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
    patronymic: str | None = Field(
        default=None,
        description="Nama keturunan terdakwa (BIN/BINTI diikuti nama ayah)",
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


class Prosecutor(BaseModel):
    """Prosecutor information."""

    name: str | None = Field(default=None, description="Nama Jaksa Penuntut Umum")
    role: str | None = Field(
        default=None,
        description="Jabatan/peran jaksa jika ada",
    )


class CourtClerk(BaseModel):
    """Court clerk information."""

    name: str | None = Field(default=None, description="Nama Panitera Pengganti")
    role: str | None = Field(
        default=None,
        description="Jabatan/peran panitera jika ada",
    )


class CourtPersonnel(BaseModel):
    """All court personnel involved in the case."""

    judges: list[Judge] | None = Field(
        default=None, description="Daftar hakim yang menangani perkara"
    )
    prosecutors: list[Prosecutor] | None = Field(
        default=None, description="Daftar Jaksa Penuntut Umum"
    )
    court_clerks: list[CourtClerk] | None = Field(
        default=None, description="Daftar Panitera Pengganti"
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
        description="Hasil putusan (guilty/not_guilty/acquitted)",
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

    legal_element_considerations: list[str] | None = Field(
        default=None,
        description="Pertimbangan hakim terhadap unsur-unsur hukum dari fakta hukum yang ada. Biasanya muncul setelah fakta hukum dengan kata kunci 'menimbang' yang membahas unsur-unsur pasal.",
    )
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
