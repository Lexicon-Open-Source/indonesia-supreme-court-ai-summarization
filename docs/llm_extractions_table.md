# LLM Extractions Table Design

## Table: `llm_extractions`

Stores the extraction results from LLM processing of court case documents.

## SQL Migration

```sql
CREATE TABLE llm_extractions (
    id VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid()::varchar,
    extraction_id VARCHAR NOT NULL,
    extraction_result JSONB,
    summary_en TEXT,
    summary_id TEXT,
    extraction_confidence FLOAT,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_llm_extractions_extraction_id
        FOREIGN KEY (extraction_id) REFERENCES extractions(id)
);

CREATE INDEX idx_llm_extractions_extraction_id ON llm_extractions(extraction_id);
CREATE INDEX idx_llm_extractions_status ON llm_extractions(status);
```

## Column Descriptions

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | VARCHAR(36) | NO | Primary key (UUID) |
| `extraction_id` | VARCHAR | NO | Foreign key to `extractions` table |
| `extraction_result` | JSONB | YES | Structured output from LLM |
| `summary_en` | TEXT | YES | Case summary in English |
| `summary_id` | TEXT | YES | Case summary in Indonesian |
| `extraction_confidence` | FLOAT | YES | Confidence score (0.0-1.0) |
| `status` | VARCHAR(50) | NO | Processing status (default: 'pending') |
| `created_at` | TIMESTAMPTZ | NO | Record creation timestamp |
| `updated_at` | TIMESTAMPTZ | NO | Last update timestamp |

## Status Values

- `pending` - Awaiting processing
- `processing` - Currently being processed by LLM
- `completed` - Successfully extracted
- `failed` - Extraction failed

---

## Extraction Result Schema (v2 - Structured)

The `extraction_result` JSONB column stores the structured output with nested objects for better organization.

### Top-Level Structure

```json
{
  "defendant": { ... },
  "legal_counsels": [ ... ],
  "court": { ... },
  "court_personnel": { ... },
  "indictment": { ... },
  "prosecution_demand": { ... },
  "defense_strategy": { ... },
  "legal_facts": { ... },
  "judicial_considerations": { ... },
  "verdict": { ... },
  "state_loss": { ... },
  "case_metadata": { ... },
  "legal_entity_analysis": { ... },
  "additional_case_data": { ... },
  "extraction_confidence": 0.95
}
```

---

### 1. Defendant (`defendant`)

Complete defendant information with structured address.

```json
{
  "defendant": {
    "name": "ISHAK EFENDI",
    "alias": null,
    "patronymic": "BIN AHMAD",
    "place_of_birth": "Api-api (Bengkalis)",
    "date_of_birth": "1963-05-03",
    "age": 70,
    "gender": "Laki-laki",
    "citizenship": "Indonesia",
    "address": {
      "street": "Jl. Putri Tujuh No. 06",
      "rt_rw": "RT 016",
      "kelurahan": "Teluk Binjai",
      "kecamatan": "Dumai Timur",
      "city": "Dumai",
      "province": "Riau",
      "full_address": "Jl. Putri Tujuh No. 06 RT 016 Kelurahan Teluk Binjai Kecamatan Dumai Timur Kota Dumai"
    },
    "religion": "Islam",
    "occupation": "Pensiunan PNS / Ketua Baznas Kota Dumai",
    "education": "S1 (Sarjana Pendidikan)"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Nama lengkap terdakwa |
| `alias` | string | Nama alias/panggilan (jika ada) |
| `patronymic` | string | Nama keturunan (BIN/BINTI diikuti nama ayah) |
| `place_of_birth` | string | Tempat lahir |
| `date_of_birth` | string | Tanggal lahir (YYYY-MM-DD) |
| `age` | integer | Umur saat putusan |
| `gender` | string | Jenis kelamin (Laki-laki/Perempuan) |
| `citizenship` | string | Kewarganegaraan |
| `address` | object | Alamat terstruktur |
| `religion` | string | Agama |
| `occupation` | string | Pekerjaan |
| `education` | string | Pendidikan terakhir |

#### Address Structure (`defendant.address`)

| Field | Type | Description |
|-------|------|-------------|
| `street` | string | Nama jalan dan nomor |
| `rt_rw` | string | RT/RW |
| `kelurahan` | string | Kelurahan/Desa |
| `kecamatan` | string | Kecamatan |
| `city` | string | Kota/Kabupaten |
| `province` | string | Provinsi |
| `full_address` | string | Alamat lengkap (fallback) |

---

### 2. Legal Counsels (`legal_counsels`)

Array of defendant's lawyers.

```json
{
  "legal_counsels": [
    {
      "name": "CASSAROLLY SINAGA, S.H., M.H",
      "office_name": "Kantor Hukum Sinaga & Partners",
      "office_address": "Jalan Lend. Sudirman No. 229, Kota Dumai, Propinsi Riau"
    },
    {
      "name": "WAHYU AWALUDIN RAHMAN, S.H., M.H, CLA",
      "office_name": null,
      "office_address": null
    }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Nama penasihat hukum |
| `office_name` | string | Nama kantor hukum |
| `office_address` | string | Alamat kantor hukum |

---

### 3. Court Information (`court`)

Court and case registration details.

```json
{
  "court": {
    "case_register_number": "49/Pid.Sus-TPK/2023/PN. Pbr",
    "verdict_number": "49/Pid.Sus-TPK/2023/PN. Pbr",
    "court_name": "Pengadilan Negeri Pekanbaru",
    "court_level": "Pengadilan Negeri",
    "province": "Riau",
    "city": "Pekanbaru"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `case_register_number` | string | Nomor register perkara |
| `verdict_number` | string | Nomor putusan |
| `court_name` | string | Nama pengadilan |
| `court_level` | string | Tingkat pengadilan (PN/PT/MA) |
| `province` | string | Provinsi lokasi pengadilan |
| `city` | string | Kota lokasi pengadilan |

---

### 4. Court Personnel (`court_personnel`)

All court officials involved in the case.

```json
{
  "court_personnel": {
    "judges": [
      { "name": "MARDISON, S.H.", "role": "Ketua Majelis" },
      { "name": "YOSI ASTUTY, S.H.", "role": "Hakim Anggota" },
      { "name": "YANUAR ANADI, S.H., M.H", "role": "Hakim Anggota" }
    ],
    "prosecutors": ["ROSLINA, S.H., M.H."],
    "court_clerks": ["DITA TRIWULANY, S.H."]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `judges` | array | Daftar hakim dengan nama dan peran |
| `prosecutors` | array | Nama-nama Jaksa Penuntut Umum |
| `court_clerks` | array | Nama-nama Panitera Pengganti |

#### Judge Structure (`court_personnel.judges[]`)

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Nama hakim |
| `role` | string | Peran (Ketua Majelis/Hakim Anggota) |

---

### 5. Indictment (`indictment`)

Prosecution's charges against the defendant.

```json
{
  "indictment": {
    "type": "Subsidiair",
    "chronology": "Terdakwa ISHAK EFFENDI selaku Ketua Badan Amil Zakat Nasional...",
    "crime_location": "Kantor Baznas Kota Dumai Jalan Jenderal Sudirman No. 170...",
    "crime_period": {
      "start_date": "2019-02-01",
      "end_date": "2021-09-30",
      "description": "antara bulan Februari tahun 2019 sampai dengan bulan September tahun 2021"
    },
    "cited_articles": [
      {
        "article": "Pasal 2 Ayat (1)",
        "law_name": "UU Pemberantasan Tindak Pidana Korupsi",
        "law_number": "31",
        "law_year": 1999,
        "full_citation": "Pasal 2 Ayat (1) UU No. 31 Tahun 1999"
      },
      {
        "article": "Pasal 55 Ayat (1) ke-1",
        "law_name": "KUHP",
        "law_number": null,
        "law_year": null,
        "full_citation": "Pasal 55 Ayat (1) ke-1 KUHP"
      }
    ],
    "defense_exception_status": "Tidak Ada"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Bentuk dakwaan (Tunggal/Alternatif/Subsidiair/Kumulatif/Campuran) |
| `chronology` | string | Ringkasan kronologis kasus |
| `crime_location` | string | Locus delicti (tempat kejadian) |
| `crime_period` | object | Tempus delicti (waktu kejadian) |
| `cited_articles` | array | Pasal-pasal yang didakwakan |
| `defense_exception_status` | string | Status eksepsi (Ditolak/Diterima/Tidak Ada) |

#### Crime Period Structure (`indictment.crime_period`)

| Field | Type | Description |
|-------|------|-------------|
| `start_date` | string | Tanggal mulai (YYYY-MM-DD) |
| `end_date` | string | Tanggal akhir (YYYY-MM-DD) |
| `description` | string | Deskripsi asli dari dokumen |

#### Cited Article Structure (`indictment.cited_articles[]`)

| Field | Type | Description |
|-------|------|-------------|
| `article` | string | Nomor pasal (contoh: Pasal 2 Ayat (1)) |
| `law_name` | string | Nama undang-undang |
| `law_number` | string | Nomor undang-undang |
| `law_year` | integer | Tahun undang-undang |
| `full_citation` | string | Kutipan lengkap |

---

### 6. Prosecution Demand (`prosecution_demand`)

Prosecutor's sentencing demands.

```json
{
  "prosecution_demand": {
    "date": "2023-12-14",
    "articles": [
      {
        "article": "Pasal 3",
        "law_name": "UU Pemberantasan Tindak Pidana Korupsi",
        "law_number": "31",
        "law_year": 1999,
        "full_citation": "Pasal 3 jo. Pasal 18 UU No. 31 Tahun 1999"
      }
    ],
    "content": "Menyatakan Terdakwa ISHAK EFFENDI terbukti bersalah...",
    "prison_sentence_months": 24,
    "prison_sentence_description": "2 (Dua) Tahun",
    "fine_amount": 100000000,
    "fine_subsidiary_confinement_months": 6,
    "restitution_amount": 176848000,
    "restitution_subsidiary_type": "penjara",
    "restitution_subsidiary_duration_months": null
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `date` | string | Tanggal tuntutan (YYYY-MM-DD) |
| `articles` | array | Pasal-pasal dalam tuntutan |
| `content` | string | Isi ringkas tuntutan |
| `prison_sentence_months` | float | Durasi penjara yang dituntut (bulan) |
| `prison_sentence_description` | string | Deskripsi hukuman penjara |
| `fine_amount` | float | Nilai denda yang dituntut (Rupiah) |
| `fine_subsidiary_confinement_months` | integer | Kurungan pengganti denda (bulan) |
| `restitution_amount` | float | Uang pengganti yang dituntut (Rupiah) |
| `restitution_subsidiary_type` | string | Jenis pidana pengganti (kurungan/penjara) |
| `restitution_subsidiary_duration_months` | integer | Durasi pidana pengganti (bulan) |

---

### 7. Defense Strategy (`defense_strategy`)

Complete defense strategy including exceptions, pleas, and rejoinders.

```json
{
  "defense_strategy": {
    "exception": {
      "filed": true,
      "date": "2023-10-15",
      "summary": "Terdakwa mengajukan eksepsi terkait kompetensi pengadilan...",
      "primary_arguments": [
        {
          "type": "competence",
          "description": "Pengadilan tidak berwenang mengadili",
          "details": "Apakah pengadilan berwenang mengadili? (Kompetensi Relatif/Absolut)"
        },
        {
          "type": "indictment_validity",
          "description": "Dakwaan kabur (Obscuur Libel)",
          "details": "Apakah dakwaan kabur (Obscuur Libel) atau cacat formil?"
        },
        {
          "type": "procedure",
          "description": "Penyidikan tidak sah",
          "details": "Apakah penyidikan sah sesuai KUHAP?"
        }
      ],
      "prosecutor_response": {
        "date": "2023-10-22",
        "summary": "JPU menolak seluruh dalil eksepsi terdakwa..."
      },
      "court_interlocutory_ruling": {
        "date": "2023-10-29",
        "decision": "Ditolak",
        "reasoning": "Majelis Hakim berpendapat eksepsi tidak beralasan hukum..."
      }
    },
    "plea": {
      "date": "2023-12-20",
      "submitted_by": ["Terdakwa", "Penasihat Hukum"],
      "personal_plea": {
        "filed": true,
        "summary": "Terdakwa memohon keringanan hukuman karena tulang punggung keluarga dan menyesali perbuatannya"
      },
      "legal_counsel_plea": {
        "filed": true,
        "summary": "Penasihat hukum memohon pembebasan karena unsur tidak terpenuhi",
        "key_arguments": [
          {
            "point": "unsur_melawan_hukum",
            "argument": "Perbuatan terdakwa tidak memenuhi unsur melawan hukum karena..."
          },
          {
            "point": "unsur_memperkaya_diri",
            "argument": "Tidak ada bukti terdakwa memperkaya diri sendiri..."
          },
          {
            "point": "procedural_flaws",
            "argument": "Terdapat cacat prosedur dalam penyidikan..."
          }
        ],
        "specific_requests": [
          "Membebaskan Terdakwa dari segala dakwaan (Vrijspraak)",
          "Melepaskan Terdakwa dari segala tuntutan hukum (Onslag van alle rechtsvervolging)"
        ]
      }
    },
    "rejoinders": {
      "replik": {
        "date": "2023-12-27",
        "summary": "JPU tetap pada tuntutan semula dan menolak dalil pledoi"
      },
      "duplik": {
        "date": "2024-01-03",
        "summary": "Penasihat hukum tetap pada pledoi dan menolak replik JPU"
      }
    }
  }
}
```

#### Exception Structure (`defense_strategy.exception`)

| Field | Type | Description |
|-------|------|-------------|
| `filed` | boolean | Apakah eksepsi diajukan |
| `date` | string | Tanggal pengajuan eksepsi (YYYY-MM-DD) |
| `summary` | string | Ringkasan eksepsi yang diajukan |
| `primary_arguments` | array | Argumen utama dalam eksepsi |
| `prosecutor_response` | object | Tanggapan JPU atas eksepsi |
| `court_interlocutory_ruling` | object | Putusan sela pengadilan |

#### Exception Argument Structure (`defense_strategy.exception.primary_arguments[]`)

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Jenis argumen (competence/indictment_validity/procedure) |
| `description` | string | Deskripsi argumen eksepsi |
| `details` | string | Detail argumen eksepsi |

#### Prosecutor Response Structure (`defense_strategy.exception.prosecutor_response`)

| Field | Type | Description |
|-------|------|-------------|
| `date` | string | Tanggal tanggapan JPU (YYYY-MM-DD) |
| `summary` | string | Ringkasan tanggapan Penuntut Umum |

#### Interlocutory Ruling Structure (`defense_strategy.exception.court_interlocutory_ruling`)

| Field | Type | Description |
|-------|------|-------------|
| `date` | string | Tanggal putusan sela (YYYY-MM-DD) |
| `decision` | string | Keputusan putusan sela (Diterima/Ditolak) |
| `reasoning` | string | Alasan/pertimbangan putusan sela |

#### Plea Structure (`defense_strategy.plea`)

| Field | Type | Description |
|-------|------|-------------|
| `date` | string | Tanggal pengajuan pledoi (YYYY-MM-DD) |
| `submitted_by` | array | Pihak yang mengajukan pledoi |
| `personal_plea` | object | Pembelaan pribadi dari terdakwa |
| `legal_counsel_plea` | object | Pembelaan yuridis dari pengacara |

#### Personal Plea Structure (`defense_strategy.plea.personal_plea`)

| Field | Type | Description |
|-------|------|-------------|
| `filed` | boolean | Apakah pembelaan pribadi diajukan |
| `summary` | string | Ringkasan pembelaan pribadi terdakwa |

#### Legal Counsel Plea Structure (`defense_strategy.plea.legal_counsel_plea`)

| Field | Type | Description |
|-------|------|-------------|
| `filed` | boolean | Apakah pledoi pengacara diajukan |
| `summary` | string | Ringkasan pembelaan yuridis |
| `key_arguments` | array | Argumen utama dalam pledoi |
| `specific_requests` | array | Permintaan spesifik (Vrijspraak/Onslag/Keringanan) |

#### Plea Argument Structure (`defense_strategy.plea.legal_counsel_plea.key_arguments[]`)

| Field | Type | Description |
|-------|------|-------------|
| `point` | string | Poin argumen (unsur_melawan_hukum/unsur_memperkaya_diri/procedural_flaws) |
| `argument` | string | Isi argumen |

#### Rejoinders Structure (`defense_strategy.rejoinders`)

| Field | Type | Description |
|-------|------|-------------|
| `replik` | object | Tanggapan Jaksa atas Pledoi |
| `duplik` | object | Tanggapan balik Pengacara atas Replik |

#### Replik/Duplik Structure (`defense_strategy.rejoinders.replik/duplik`)

| Field | Type | Description |
|-------|------|-------------|
| `date` | string | Tanggal replik/duplik (YYYY-MM-DD) |
| `summary` | string | Ringkasan tanggapan |

---

### 8. Legal Facts (`legal_facts`)

Categorized legal facts established during trial.

```json
{
  "legal_facts": {
    "organizational_structure": [
      "Terdakwa ISHAK EFFENDI selaku Ketua Baznas Kota Dumai...",
      "Struktur kepengurusan Baznas Kota Dumai..."
    ],
    "standard_procedures": [
      "Pendistribusian zakat seharusnya berpedoman pada Peraturan Baznas Nomor 3 Tahun 2018...",
      "Prosedur pencairan dana meliputi..."
    ],
    "violations": [
      "Proses pencairan tanpa proposal atau formulir permohonan...",
      "Pendistribusian dana tanpa verifikasi lengkap..."
    ],
    "financial_irregularities": [
      "Dana zakat tidak diterima sama sekali oleh Mustahik (fiktif) sebesar Rp608.700.000...",
      "Pemotongan dana zakat sebesar Rp382.465.000..."
    ],
    "witness_testimonies": [
      "Keterangan saksi INDRA SYAHRIL...",
      "Keterangan saksi ISMAN JAYA NST..."
    ],
    "documentary_evidence": [
      "Laporan Hasil Audit Inspektorat Kota Dumai...",
      "Surat Keputusan Pengurus Baznas..."
    ],
    "other_facts": []
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `organizational_structure` | array | Fakta struktur organisasi |
| `standard_procedures` | array | Prosedur standar yang seharusnya |
| `violations` | array | Pelanggaran yang dilakukan |
| `financial_irregularities` | array | Penyimpangan keuangan |
| `witness_testimonies` | array | Keterangan saksi |
| `documentary_evidence` | array | Bukti dokumen |
| `other_facts` | array | Fakta lainnya |

---

### 9. Judicial Considerations (`judicial_considerations`)

Judge's considerations in sentencing.

```json
{
  "judicial_considerations": {
    "legal_element_considerations": [
      "Menimbang, bahwa unsur 'Setiap Orang' telah terpenuhi karena terdakwa adalah subjek hukum yang mampu bertanggung jawab",
      "Menimbang, bahwa unsur 'secara melawan hukum' telah terbukti berdasarkan fakta-fakta hukum di persidangan",
      "Menimbang, bahwa unsur 'memperkaya diri sendiri atau orang lain atau suatu korporasi' telah terpenuhi"
    ],
    "aggravating_factors": [
      "Perbuatan terdakwa bertentangan dengan kebijakan pemerintah yang gencar memberantas tindak pidana korupsi"
    ],
    "mitigating_factors": [
      "Terdakwa belum pernah dihukum",
      "Terdakwa bersikap sopan di persidangan",
      "Terdakwa telah mengembalikan uang hasil korupsi"
    ]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `legal_element_considerations` | array | Pertimbangan hakim terhadap unsur-unsur hukum (biasanya dengan keyword "menimbang" yang membahas unsur pasal) |
| `aggravating_factors` | array | Hal yang memberatkan |
| `mitigating_factors` | array | Hal yang meringankan |

---

### 10. Verdict (`verdict`)

Final court decision.

```json
{
  "verdict": {
    "number": "49/Pid.Sus-TPK/2023/PN. Pbr",
    "date": "2024-01-18",
    "day": "Kamis",
    "year": 2024,
    "result": "guilty",
    "primary_charge_proven": false,
    "subsidiary_charge_proven": true,
    "proven_articles": [
      {
        "article": "Pasal 3",
        "law_name": "UU Pemberantasan Tindak Pidana Korupsi",
        "law_number": "31",
        "law_year": 1999,
        "full_citation": "Pasal 3 UU No. 31 Tahun 1999"
      },
      {
        "article": "Pasal 18",
        "law_name": "UU Pemberantasan Tindak Pidana Korupsi",
        "law_number": "31",
        "law_year": 1999,
        "full_citation": "Pasal 18 UU No. 31 Tahun 1999"
      },
      {
        "article": "Pasal 55 Ayat (1) ke-1",
        "law_name": "KUHP",
        "law_number": null,
        "law_year": null,
        "full_citation": "Pasal 55 Ayat (1) ke-1 KUHP"
      },
      {
        "article": "Pasal 64 Ayat (1)",
        "law_name": "KUHP",
        "law_number": null,
        "law_year": null,
        "full_citation": "Pasal 64 Ayat (1) KUHP"
      }
    ],
    "ruling_contents": [
      "Menyatakan terdakwa ISHAK EFENDI tidak terbukti secara sah dan meyakinkan bersalah melakukan tindak pidana korupsi sebagaimana disebut dalam dakwaan primair",
      "Membebaskan terdakwa dari dakwaan primair tersebut",
      "Menyatakan terdakwa ISHAK EFENDI terbukti bersalah turut serta melakukan tindak pidana korupsi secara berlanjut",
      "Menjatuhkan Pidana terhadap Terdakwa ISHAK EFENDI dengan pidana penjara selama 1 (satu) tahun dan 4 (empat) bulan..."
    ],
    "sentences": {
      "imprisonment": {
        "duration_months": 16,
        "description": "1 (satu) tahun dan 4 (empat) bulan"
      },
      "fine": {
        "amount": 50000000,
        "subsidiary_confinement_months": 2
      },
      "restitution": {
        "amount": 176848000,
        "already_paid": 176848000,
        "remaining": 0,
        "subsidiary_type": null,
        "subsidiary_duration_months": null
      }
    }
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `number` | string | Nomor putusan |
| `date` | string | Tanggal putusan (YYYY-MM-DD) |
| `day` | string | Hari pembacaan putusan |
| `year` | integer | Tahun putusan |
| `result` | string | Hasil putusan (guilty/not_guilty/acquitted) |
| `primary_charge_proven` | boolean | Dakwaan primer terbukti? |
| `subsidiary_charge_proven` | boolean | Dakwaan subsidiair terbukti? |
| `proven_articles` | array | Pasal-pasal yang terbukti (termasuk juncto) - menggunakan struktur CitedArticle |
| `ruling_contents` | array | Isi amar putusan |
| `sentences` | object | Detail hukuman |

#### Verdict Sentences Structure (`verdict.sentences`)

```json
{
  "sentences": {
    "imprisonment": {
      "duration_months": 16,
      "description": "1 tahun 4 bulan"
    },
    "fine": {
      "amount": 50000000,
      "subsidiary_confinement_months": 2
    },
    "restitution": {
      "amount": 176848000,
      "already_paid": 176848000,
      "remaining": 0,
      "subsidiary_type": "penjara",
      "subsidiary_duration_months": 6
    }
  }
}
```

---

### 11. State Loss (`state_loss`)

Information about state financial loss.

```json
{
  "state_loss": {
    "auditor": "Inspektorat Kota Dumai",
    "audit_report_number": "790.04.451/37/INSPE",
    "audit_report_date": "2023-06-05",
    "indicted_amount": 1419805500,
    "proven_amount": 1419805500,
    "returned_amount": 176848000,
    "remaining_due": 0,
    "currency": "IDR",
    "perpetrators_proceeds": [
      {
        "name": "ISHAK EFFENDI",
        "amount": 176848500,
        "role": "Ketua Baznas Kota Dumai"
      },
      {
        "name": "INDRA SYAHRIL",
        "amount": 1102019000,
        "role": "Bendahara Pengeluaran"
      },
      {
        "name": "ISMAN JAYA NST",
        "amount": 82365000,
        "role": "Wakil Ketua II"
      }
    ]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `auditor` | string | Instansi pengaudit (BPK/BPKP/Inspektorat) |
| `audit_report_number` | string | Nomor laporan audit |
| `audit_report_date` | string | Tanggal laporan audit |
| `indicted_amount` | float | Kerugian yang didakwakan (Rupiah) |
| `proven_amount` | float | Kerugian yang terbukti (Rupiah) |
| `returned_amount` | float | Jumlah yang dikembalikan (Rupiah) |
| `remaining_due` | float | Sisa yang wajib dikembalikan (Rupiah) |
| `currency` | string | Mata uang (default: IDR) |
| `perpetrators_proceeds` | array | Rincian per pelaku |

#### Perpetrator Proceeds Structure (`state_loss.perpetrators_proceeds[]`)

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Nama pelaku |
| `amount` | float | Jumlah yang diperoleh (Rupiah) |
| `role` | string | Jabatan/peran dalam kasus |

---

### 12. Case Metadata (`case_metadata`)

Additional case classification and related cases.

```json
{
  "case_metadata": {
    "crime_category": "Korupsi",
    "crime_subcategory": "Penyalahgunaan Wewenang",
    "institution_involved": "BAZNAS Kota Dumai",
    "related_cases": [
      {
        "defendant_name": "ISMAN JAYA NST",
        "case_number": null,
        "status": "separate_prosecution",
        "relationship": "turut serta"
      },
      {
        "defendant_name": "INDRA SYAHRIL",
        "case_number": null,
        "status": "separate_prosecution",
        "relationship": "turut serta"
      }
    ]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `crime_category` | string | Kategori tindak pidana |
| `crime_subcategory` | string | Subkategori tindak pidana |
| `institution_involved` | string | Instansi/lembaga terlibat |
| `related_cases` | array | Perkara-perkara terkait |

#### Related Case Structure (`case_metadata.related_cases[]`)

| Field | Type | Description |
|-------|------|-------------|
| `defendant_name` | string | Nama terdakwa perkara terkait |
| `case_number` | string | Nomor perkara terkait |
| `status` | string | Status (separate_prosecution/splitsing/joined) |
| `relationship` | string | Hubungan (turut serta/membantu/menyuruh melakukan) |

---

### 13. Legal Entity Analysis (`legal_entity_analysis`)

Analysis of legal entities (organizations, companies, institutions) involved in the case and their affiliations with persons.

```json
{
  "legal_entity_analysis": {
    "entity_registry": [
      {
        "id": "ENT_01",
        "name": "SMP Negeri 5 Pallangga",
        "legal_form": "Instansi Pemerintah",
        "sector": "Pendidikan",
        "role_in_case": "Locus Delicti (Tempat Kejadian) & Korban",
        "address": "Jalan Baso Dg. Mangawing, Kabupaten Gowa"
      },
      {
        "id": "ENT_02",
        "name": "PT. Heksa Prima Abadi",
        "legal_form": "Perseroan Terbatas (PT)",
        "sector": "Swasta / Vendor",
        "role_in_case": "Penyedia Barang (Buku) - Terindikasi Fiktif/Mark-up"
      },
      {
        "id": "ENT_03",
        "name": "CV. Karsa Mandiri",
        "legal_form": "Comanditaire Venootschap (CV)",
        "sector": "Swasta / Vendor",
        "role_in_case": "Penyedia Barang (Buku)"
      },
      {
        "id": "ENT_04",
        "name": "Inspektorat Daerah Kabupaten Gowa",
        "legal_form": "Lembaga Pemerintah",
        "sector": "Pengawasan",
        "role_in_case": "Auditor (Penghitung Kerugian Negara)"
      }
    ],
    "affiliations_map": [
      {
        "person_name": "Drs. H. JAMALUDDIN",
        "related_entity_id": "ENT_01",
        "position": "Kepala Sekolah / KPA",
        "nature_of_relationship": "Struktural (Pejabat Berwenang)"
      },
      {
        "person_name": "Saparuddin",
        "related_entity_id": "ENT_02",
        "position": "Direktur/Pengurus",
        "nature_of_relationship": "Kepemilikan & Pengendali"
      },
      {
        "person_name": "Saparuddin",
        "related_entity_id": "ENT_03",
        "position": "Direktur/Pengurus",
        "nature_of_relationship": "Kepemilikan & Pengendali (Indikasi Pinjam Bendera)"
      },
      {
        "person_name": "Tim Audit",
        "related_entity_id": "ENT_04",
        "position": "Auditor",
        "nature_of_relationship": "Penugasan Resmi"
      }
    ]
  }
}
```

#### Entity Registry Structure (`legal_entity_analysis.entity_registry[]`)

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique identifier for the entity (e.g., ENT_01) |
| `name` | string | Nama entitas/badan hukum |
| `legal_form` | string | Bentuk hukum (Instansi Pemerintah/PT/CV/Yayasan/dll) |
| `sector` | string | Sektor entitas (Pendidikan/Swasta/Pengawasan/dll) |
| `role_in_case` | string | Peran entitas dalam kasus (Locus Delicti/Korban/Vendor/Auditor/dll) |
| `address` | string | Alamat entitas |

#### Affiliations Map Structure (`legal_entity_analysis.affiliations_map[]`)

| Field | Type | Description |
|-------|------|-------------|
| `person_name` | string | Nama orang atau pihak yang berafiliasi |
| `related_entity_id` | string | ID entitas terkait (merujuk ke entity_registry.id) |
| `position` | string | Jabatan/posisi dalam entitas (Direktur/Kepala/Auditor/dll) |
| `nature_of_relationship` | string | Sifat hubungan (Struktural/Kepemilikan/Penugasan Resmi/dll) |

---

### 14. Additional Case Data (`additional_case_data`)

Additional data for complex cases such as appeal cases (banding/kasasi) and multi-stage proceedings.

```json
{
  "additional_case_data": {
    "detention_history": [
      {
        "stage": "Penyidik",
        "start_date": "2023-01-15",
        "end_date": "2023-02-03",
        "duration_days": 20,
        "location": "Rutan Kelas I Pekanbaru"
      },
      {
        "stage": "Penuntut Umum",
        "start_date": "2023-02-04",
        "end_date": "2023-02-23",
        "duration_days": 20,
        "location": "Rutan Kelas I Pekanbaru"
      },
      {
        "stage": "Hakim Pengadilan Negeri",
        "start_date": "2023-02-24",
        "end_date": "2023-03-25",
        "duration_days": 30,
        "location": "Rutan Kelas I Pekanbaru"
      }
    ],
    "lower_court_decision": {
      "court_name": "Pengadilan Negeri Pekanbaru",
      "verdict_number": "49/Pid.Sus-TPK/2023/PN.Pbr",
      "verdict_date": "2024-01-18",
      "primary_charge_ruling": "Tidak Terbukti",
      "subsidiary_charge_ruling": "Terbukti",
      "sentence": {
        "imprisonment": "1 tahun 4 bulan",
        "fine": "Rp50.000.000,- subsidair 2 bulan kurungan",
        "restitution": "Rp176.848.000,-"
      }
    },
    "appeal_process": {
      "applicant": "Penuntut Umum",
      "request_date": "2024-01-22",
      "registration_date": "2024-01-23",
      "notification_to_defendant": "2024-01-25",
      "notification_to_prosecutor": null,
      "memorandum_filed": true,
      "memorandum_date": "2024-02-05",
      "contra_memorandum_filed": true,
      "contra_memorandum_date": "2024-02-12",
      "judge_notes": "Pemohon banding tidak puas dengan putusan pidana penjara yang dijatuhkan"
    },
    "evidence_inventory": {
      "returned_to_defendant": [
        {
          "item": "1 (satu) unit sepeda motor Honda Vario",
          "recipient": "Terdakwa",
          "condition": "Baik",
          "status": "Dikembalikan"
        }
      ],
      "returned_to_third_party": [
        {
          "item": "Dokumen Surat Keputusan Pengangkatan",
          "recipient": "BAZNAS Kota Dumai",
          "condition": "Baik",
          "status": "Dikembalikan"
        }
      ],
      "confiscated_for_state": [
        {
          "item": "Uang tunai Rp176.848.000,-",
          "recipient": null,
          "condition": null,
          "status": "Dirampas untuk negara"
        }
      ],
      "destroyed": [],
      "attached_to_case_file": [
        {
          "item": "Fotokopi Laporan Keuangan Tahun 2019-2021",
          "recipient": null,
          "condition": null,
          "status": "Tetap terlampir dalam berkas"
        }
      ],
      "used_in_other_case": []
    }
  }
}
```

#### Detention Period Structure (`additional_case_data.detention_history[]`)

| Field | Type | Description |
|-------|------|-------------|
| `stage` | string | Tahapan penahanan (Penyidik/Penuntut Umum/Hakim/Perpanjangan) |
| `start_date` | string | Tanggal mulai penahanan (YYYY-MM-DD) |
| `end_date` | string | Tanggal berakhir penahanan (YYYY-MM-DD) |
| `duration_days` | integer | Durasi penahanan dalam hari |
| `location` | string | Lokasi penahanan (Rutan/Lapas/Tahanan Kota) |

#### Lower Court Decision Structure (`additional_case_data.lower_court_decision`)

| Field | Type | Description |
|-------|------|-------------|
| `court_name` | string | Nama pengadilan tingkat pertama |
| `verdict_number` | string | Nomor putusan pengadilan tingkat pertama |
| `verdict_date` | string | Tanggal putusan (YYYY-MM-DD) |
| `primary_charge_ruling` | string | Putusan dakwaan primer (Terbukti/Tidak Terbukti/Bebas) |
| `subsidiary_charge_ruling` | string | Putusan dakwaan subsidiair (Terbukti/Tidak Terbukti) |
| `sentence` | object | Detail hukuman dari pengadilan tingkat pertama |

#### Lower Court Sentence Structure (`additional_case_data.lower_court_decision.sentence`)

| Field | Type | Description |
|-------|------|-------------|
| `imprisonment` | string | Hukuman penjara dari pengadilan tingkat pertama |
| `fine` | string | Denda dari pengadilan tingkat pertama |
| `restitution` | string | Uang pengganti dari pengadilan tingkat pertama |

#### Appeal Process Structure (`additional_case_data.appeal_process`)

| Field | Type | Description |
|-------|------|-------------|
| `applicant` | string | Pihak yang mengajukan banding (Penuntut Umum/Terdakwa/Keduanya) |
| `request_date` | string | Tanggal permohonan banding (YYYY-MM-DD) |
| `registration_date` | string | Tanggal registrasi banding (YYYY-MM-DD) |
| `notification_to_defendant` | string | Tanggal pemberitahuan kepada terdakwa (YYYY-MM-DD) |
| `notification_to_prosecutor` | string | Tanggal pemberitahuan kepada JPU (YYYY-MM-DD) |
| `memorandum_filed` | boolean | Apakah memori banding diajukan |
| `memorandum_date` | string | Tanggal pengajuan memori banding (YYYY-MM-DD) |
| `contra_memorandum_filed` | boolean | Apakah kontra memori banding diajukan |
| `contra_memorandum_date` | string | Tanggal pengajuan kontra memori banding (YYYY-MM-DD) |
| `judge_notes` | string | Catatan hakim terkait proses banding |

#### Evidence Inventory Structure (`additional_case_data.evidence_inventory`)

| Field | Type | Description |
|-------|------|-------------|
| `returned_to_defendant` | array | Barang bukti yang dikembalikan kepada terdakwa |
| `returned_to_third_party` | array | Barang bukti yang dikembalikan kepada pihak ketiga |
| `confiscated_for_state` | array | Barang bukti yang dirampas untuk negara |
| `destroyed` | array | Barang bukti yang dimusnahkan |
| `attached_to_case_file` | array | Barang bukti yang tetap terlampir dalam berkas |
| `used_in_other_case` | array | Barang bukti yang digunakan dalam perkara lain |

#### Evidence Item Structure (`additional_case_data.evidence_inventory.*[]`)

| Field | Type | Description |
|-------|------|-------------|
| `item` | string | Deskripsi barang bukti |
| `recipient` | string | Penerima barang bukti (jika dikembalikan) |
| `condition` | string | Kondisi/status barang bukti |
| `status` | string | Status disposisi barang bukti |

---

## Enums Reference

### Verdict Result

| Value | Description |
|-------|-------------|
| `guilty` | Terbukti bersalah |
| `not_guilty` | Bebas |
| `acquitted` | Lepas dari segala tuntutan |

### Indictment Type

| Value | Description |
|-------|-------------|
| `Tunggal` | Dakwaan tunggal |
| `Alternatif` | Dakwaan alternatif |
| `Subsidiair` | Dakwaan primer-subsidiair |
| `Kumulatif` | Dakwaan kumulatif |
| `Campuran` | Dakwaan campuran |

### Confinement Type

| Value | Description |
|-------|-------------|
| `kurungan` | Pidana kurungan |
| `penjara` | Pidana penjara |

---

## Complete Example

```json
{
  "defendant": {
    "name": "ISHAK EFENDI",
    "alias": null,
    "patronymic": "BIN AHMAD",
    "place_of_birth": "Api-api (Bengkalis)",
    "date_of_birth": "1963-05-03",
    "age": 70,
    "gender": "Laki-laki",
    "citizenship": "Indonesia",
    "address": {
      "street": "Jl. Putri Tujuh No. 06",
      "rt_rw": "RT 016",
      "kelurahan": "Teluk Binjai",
      "kecamatan": "Dumai Timur",
      "city": "Dumai",
      "province": "Riau",
      "full_address": "Jl. Putri Tujuh No. 06 RT 016 Kelurahan Teluk Binjai Kecamatan Dumai Timur Kota Dumai"
    },
    "religion": "Islam",
    "occupation": "Pensiunan PNS / Ketua Baznas Kota Dumai",
    "education": "S1 (Sarjana Pendidikan)"
  },
  "legal_counsels": [
    {
      "name": "CASSAROLLY SINAGA, S.H., M.H",
      "office_name": null,
      "office_address": "Jalan Lend. Sudirman No. 229, Kota Dumai, Propinsi Riau"
    }
  ],
  "court": {
    "case_register_number": "49/Pid.Sus-TPK/2023/PN. Pbr",
    "verdict_number": "49/Pid.Sus-TPK/2023/PN. Pbr",
    "court_name": "Pengadilan Negeri Pekanbaru",
    "court_level": "Pengadilan Negeri",
    "province": "Riau",
    "city": "Pekanbaru"
  },
  "court_personnel": {
    "judges": [
      { "name": "MARDISON, S.H.", "role": "Ketua Majelis" },
      { "name": "YOSI ASTUTY, S.H.", "role": "Hakim Anggota" },
      { "name": "YANUAR ANADI, S.H., M.H", "role": "Hakim Anggota" }
    ],
    "prosecutors": ["ROSLINA, S.H., M.H."],
    "court_clerks": ["DITA TRIWULANY, S.H."]
  },
  "indictment": {
    "type": "Subsidiair",
    "chronology": "Terdakwa ISHAK EFFENDI selaku Ketua Badan Amil Zakat Nasional...",
    "crime_location": "Kantor Baznas Kota Dumai Jalan Jenderal Sudirman No. 170...",
    "crime_period": {
      "start_date": "2019-02-01",
      "end_date": "2021-09-30",
      "description": "antara bulan Februari tahun 2019 sampai dengan bulan September tahun 2021"
    },
    "cited_articles": [
      {
        "article": "Pasal 2 Ayat (1)",
        "law_name": "UU Pemberantasan Tindak Pidana Korupsi",
        "law_number": "31",
        "law_year": 1999,
        "full_citation": "Pasal 2 Ayat (1) UU No. 31 Tahun 1999"
      }
    ],
    "defense_exception_status": null
  },
  "prosecution_demand": {
    "date": "2023-12-14",
    "articles": [],
    "content": "Menyatakan Terdakwa ISHAK EFFENDI terbukti bersalah...",
    "prison_sentence_months": 24,
    "prison_sentence_description": "2 (Dua) Tahun",
    "fine_amount": 100000000,
    "fine_subsidiary_confinement_months": 6,
    "restitution_amount": 176848000,
    "restitution_subsidiary_type": null,
    "restitution_subsidiary_duration_months": null
  },
  "defense_strategy": {
    "exception": {
      "filed": false,
      "date": null,
      "summary": null,
      "primary_arguments": [],
      "prosecutor_response": null,
      "court_interlocutory_ruling": null
    },
    "plea": {
      "date": "2023-12-20",
      "submitted_by": ["Terdakwa", "Penasihat Hukum"],
      "personal_plea": {
        "filed": true,
        "summary": "Terdakwa memohon keringanan hukuman"
      },
      "legal_counsel_plea": {
        "filed": true,
        "summary": "Penasihat hukum memohon pembebasan",
        "key_arguments": [
          { "point": "unsur_melawan_hukum", "argument": "Tidak terbukti unsur melawan hukum" }
        ],
        "specific_requests": ["Membebaskan Terdakwa dari segala dakwaan"]
      }
    },
    "rejoinders": {
      "replik": { "date": "2023-12-27", "summary": "JPU tetap pada tuntutan" },
      "duplik": { "date": "2024-01-03", "summary": "Penasihat hukum tetap pada pledoi" }
    }
  },
  "legal_facts": {
    "organizational_structure": ["..."],
    "standard_procedures": ["..."],
    "violations": ["..."],
    "financial_irregularities": ["..."],
    "witness_testimonies": ["..."],
    "documentary_evidence": ["..."],
    "other_facts": []
  },
  "judicial_considerations": {
    "aggravating_factors": [
      "Perbuatan terdakwa bertentangan dengan kebijakan pemerintah..."
    ],
    "mitigating_factors": []
  },
  "verdict": {
    "number": "49/Pid.Sus-TPK/2023/PN. Pbr",
    "date": "2024-01-18",
    "day": "Kamis",
    "year": 2024,
    "result": "guilty",
    "primary_charge_proven": false,
    "subsidiary_charge_proven": true,
    "proven_articles": [
      { "article": "Pasal 3", "law_name": "UU Pemberantasan Tindak Pidana Korupsi", "law_number": "31", "law_year": 1999, "full_citation": "Pasal 3 UU No. 31 Tahun 1999" },
      { "article": "Pasal 18", "law_name": "UU Pemberantasan Tindak Pidana Korupsi", "law_number": "31", "law_year": 1999, "full_citation": "Pasal 18 UU No. 31 Tahun 1999" },
      { "article": "Pasal 55 Ayat (1) ke-1", "law_name": "KUHP", "full_citation": "Pasal 55 Ayat (1) ke-1 KUHP" },
      { "article": "Pasal 64 Ayat (1)", "law_name": "KUHP", "full_citation": "Pasal 64 Ayat (1) KUHP" }
    ],
    "ruling_contents": ["..."],
    "sentences": {
      "imprisonment": {
        "duration_months": 16,
        "description": "1 (satu) tahun dan 4 (empat) bulan"
      },
      "fine": {
        "amount": 50000000,
        "subsidiary_confinement_months": 2
      },
      "restitution": {
        "amount": 176848000,
        "already_paid": 176848000,
        "remaining": 0,
        "subsidiary_type": null,
        "subsidiary_duration_months": null
      }
    }
  },
  "state_loss": {
    "auditor": "Inspektorat Kota Dumai",
    "audit_report_number": "790.04.451/37/INSPE",
    "audit_report_date": "2023-06-05",
    "indicted_amount": 1419805500,
    "proven_amount": 1419805500,
    "returned_amount": 176848000,
    "remaining_due": 0,
    "currency": "IDR",
    "perpetrators_proceeds": [
      { "name": "ISHAK EFFENDI", "amount": 176848500, "role": "Ketua Baznas" },
      { "name": "INDRA SYAHRIL", "amount": 1102019000, "role": "Bendahara" },
      { "name": "ISMAN JAYA NST", "amount": 82365000, "role": "Wakil Ketua II" }
    ]
  },
  "case_metadata": {
    "crime_category": "Korupsi",
    "crime_subcategory": "Penyalahgunaan Wewenang",
    "institution_involved": "BAZNAS Kota Dumai",
    "related_cases": [
      { "defendant_name": "ISMAN JAYA NST", "case_number": null, "status": "separate_prosecution", "relationship": "turut serta" },
      { "defendant_name": "INDRA SYAHRIL", "case_number": null, "status": "separate_prosecution", "relationship": "turut serta" }
    ]
  },
  "legal_entity_analysis": {
    "entity_registry": [
      { "id": "ENT_01", "name": "BAZNAS Kota Dumai", "legal_form": "Lembaga Negara", "sector": "Pengelolaan Zakat", "role_in_case": "Locus Delicti & Korban", "address": "Jalan Jenderal Sudirman No. 170, Kota Dumai" }
    ],
    "affiliations_map": [
      { "person_name": "ISHAK EFFENDI", "related_entity_id": "ENT_01", "position": "Ketua", "nature_of_relationship": "Struktural (Pejabat Berwenang)" },
      { "person_name": "INDRA SYAHRIL", "related_entity_id": "ENT_01", "position": "Bendahara Pengeluaran", "nature_of_relationship": "Struktural" },
      { "person_name": "ISMAN JAYA NST", "related_entity_id": "ENT_01", "position": "Wakil Ketua II", "nature_of_relationship": "Struktural" }
    ]
  },
  "additional_case_data": {
    "detention_history": [
      { "stage": "Penyidik", "start_date": "2023-01-15", "end_date": "2023-02-03", "duration_days": 20, "location": "Rutan Kelas I Pekanbaru" },
      { "stage": "Penuntut Umum", "start_date": "2023-02-04", "end_date": "2023-02-23", "duration_days": 20, "location": "Rutan Kelas I Pekanbaru" }
    ],
    "lower_court_decision": null,
    "appeal_process": null,
    "evidence_inventory": {
      "returned_to_defendant": [],
      "returned_to_third_party": [],
      "confiscated_for_state": [
        { "item": "Uang tunai Rp176.848.000,-", "recipient": null, "condition": null, "status": "Dirampas untuk negara" }
      ],
      "destroyed": [],
      "attached_to_case_file": [],
      "used_in_other_case": []
    }
  },
  "extraction_confidence": 0.98
}
```
