"""
Test script for the Virtual Judicial Council feature.

Tests:
1. Case parsing from text to structured data
2. Similar case search using embeddings
3. Agent deliberation
4. Opinion generation
"""

import asyncio
import logging
from datetime import datetime

from sqlalchemy.ext.asyncio import create_async_engine

from settings import get_settings
from src.council.agents.orchestrator import get_agent_orchestrator
from src.council.database import CaseDatabase, get_session_store
from src.council.schemas import CaseInput, InputType, SessionStatus
from src.council.services.case_parser import get_case_parser_service
from src.council.services.embeddings import get_council_embedding_service
from src.council.services.opinion_generator import get_opinion_generator_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Sample case text for testing
SAMPLE_CASE_TEXT = """
Terdakwa AHMAD bin SUPARNO, seorang pegawai swasta berusia 35 tahun,
ditangkap dengan membawa 2,5 gram shabu-shabu yang disimpan dalam kantong celananya.
Menurut keterangan terdakwa, barang bukti tersebut adalah untuk pemakaian pribadi.
Terdakwa sebelumnya tidak pernah dihukum dan memiliki tanggungan 2 orang anak.

Jaksa penuntut umum mendakwa terdakwa dengan Pasal 127 ayat (1) huruf a
Undang-Undang Nomor 35 Tahun 2009 tentang Narkotika, dengan ancaman hukuman
maksimal 4 tahun penjara.

Terdakwa mengaku bersalah dan menyatakan penyesalan atas perbuatannya.
Terdakwa berjanji tidak akan mengulangi perbuatannya dan bersedia
mengikuti program rehabilitasi.
"""


async def test_embedding_service():
    """Test the embedding service."""
    logger.info("=" * 60)
    logger.info("Testing Embedding Service")
    logger.info("=" * 60)

    service = get_council_embedding_service()

    # Test embedding generation
    test_text = "Kasus narkotika shabu-shabu untuk pemakaian pribadi"
    logger.info(f"Generating embedding for: '{test_text}'")

    embedding = await service.generate_embedding(test_text)
    logger.info(f"Generated embedding with {len(embedding)} dimensions")

    assert len(embedding) > 0, "Embedding should have dimensions"
    logger.info("✓ Embedding generation successful")

    return True


async def test_case_parser():
    """Test case parsing from text."""
    logger.info("=" * 60)
    logger.info("Testing Case Parser")
    logger.info("=" * 60)

    parser = get_case_parser_service()

    logger.info("Parsing sample case text...")
    case_input = await parser.parse_case(SAMPLE_CASE_TEXT)

    logger.info(f"Case type: {case_input.parsed_case.case_type.value}")
    logger.info(f"Summary: {case_input.parsed_case.summary[:100]}...")
    logger.info(f"Key facts: {len(case_input.parsed_case.key_facts)} found")
    logger.info(f"Charges: {case_input.parsed_case.charges}")

    if case_input.parsed_case.narcotics:
        n = case_input.parsed_case.narcotics
        logger.info(
            f"Narcotics details: {n.substance}, {n.weight_grams}g, "
            f"intent={n.intent.value}"
        )

    if case_input.parsed_case.defendant_profile:
        d = case_input.parsed_case.defendant_profile
        logger.info(f"Defendant: first_offender={d.is_first_offender}, age={d.age}")

    logger.info("✓ Case parsing successful")
    return case_input


async def test_similar_case_search(db_engine, case_input: CaseInput):
    """Test finding similar cases."""
    logger.info("=" * 60)
    logger.info("Testing Similar Case Search")
    logger.info("=" * 60)

    case_db = CaseDatabase(db_engine)

    logger.info("Searching for similar cases...")
    similar_cases = await case_db.find_similar_cases(case_input, limit=5)

    if similar_cases:
        logger.info(f"Found {len(similar_cases)} similar cases:")
        for i, case in enumerate(similar_cases, 1):
            logger.info(
                f"  {i}. {case.case_number} "
                f"(similarity: {case.similarity_score:.3f})"
            )
            logger.info(f"     Verdict: {case.verdict_summary}")
            logger.info(f"     Sentence: {case.sentence_months} months")
    else:
        logger.warning("No similar cases found (embeddings may need to be generated)")

    logger.info("✓ Similar case search completed")
    return similar_cases


async def test_agent_deliberation(case_input: CaseInput, similar_cases: list):
    """Test agent deliberation."""
    logger.info("=" * 60)
    logger.info("Testing Agent Deliberation")
    logger.info("=" * 60)

    store = get_session_store()
    orchestrator = get_agent_orchestrator()

    # Create session
    session = store.create_session(case_input=case_input)
    store.set_similar_cases(session.id, similar_cases)
    logger.info(f"Created session: {session.id}")

    # Generate initial opinions
    logger.info("Generating initial opinions from all judges...")
    initial_messages = await orchestrator.generate_initial_opinions(
        session_id=session.id,
        case_input=case_input.parsed_case,
        similar_cases=similar_cases,
    )

    for msg in initial_messages:
        agent_name = msg.sender.agent_id.value if hasattr(msg.sender, "agent_id") else "Unknown"
        logger.info(f"\n[{agent_name.upper()}]")
        logger.info(f"{msg.content[:500]}...")
        if msg.cited_cases:
            logger.info(f"  Cited cases: {msg.cited_cases}")
        if msg.cited_laws:
            logger.info(f"  Cited laws: {msg.cited_laws}")

    store.add_messages(session.id, initial_messages)
    logger.info("✓ Initial opinions generated")

    # Test user message
    logger.info("\nSending user message...")
    user_msg_text = "What would be an appropriate sentence considering the defendant is a first offender?"
    user_msg, responses = await orchestrator.process_user_message(
        session_id=session.id,
        user_message=user_msg_text,
        case_input=case_input.parsed_case,
        similar_cases=similar_cases,
        history=session.messages + initial_messages,
        target_agent=None,
    )

    logger.info(f"\n[USER]: {user_msg.content}")
    for response in responses:
        agent_name = response.sender.agent_id.value if hasattr(response.sender, "agent_id") else "Unknown"
        logger.info(f"\n[{agent_name.upper()}]")
        logger.info(f"{response.content[:400]}...")

    store.add_message(session.id, user_msg)
    store.add_messages(session.id, responses)

    logger.info("✓ Agent deliberation successful")
    return session


async def test_opinion_generation(session):
    """Test legal opinion generation."""
    logger.info("=" * 60)
    logger.info("Testing Opinion Generation")
    logger.info("=" * 60)

    store = get_session_store()
    updated_session = store.get_session(session.id)

    if len(updated_session.messages) < 3:
        logger.warning("Not enough messages for opinion generation, skipping...")
        return None

    generator = get_opinion_generator_service()

    logger.info("Generating legal opinion...")
    opinion = await generator.generate_opinion(
        session_id=session.id,
        case_input=updated_session.case_input.parsed_case,
        similar_cases=updated_session.similar_cases,
        messages=updated_session.messages,
        include_dissent=True,
    )

    logger.info(f"\n=== LEGAL OPINION ===")
    logger.info(f"Case Summary: {opinion.case_summary[:200]}...")
    logger.info(f"\nVerdict: {opinion.verdict_recommendation.decision.value}")
    logger.info(f"Confidence: {opinion.verdict_recommendation.confidence}")
    logger.info(f"Reasoning: {opinion.verdict_recommendation.reasoning[:200]}...")

    sr = opinion.sentence_recommendation
    logger.info(f"\nSentence Recommendation:")
    logger.info(
        f"  Imprisonment: {sr.imprisonment_months.minimum}-"
        f"{sr.imprisonment_months.maximum} months "
        f"(recommended: {sr.imprisonment_months.recommended})"
    )
    logger.info(
        f"  Fine: Rp {sr.fine_idr.minimum:,}-{sr.fine_idr.maximum:,} "
        f"(recommended: Rp {sr.fine_idr.recommended:,})"
    )

    if opinion.dissenting_views:
        logger.info(f"\nDissenting views: {len(opinion.dissenting_views)}")
        for view in opinion.dissenting_views:
            logger.info(f"  - {view[:100]}...")

    logger.info("✓ Opinion generation successful")
    return opinion


async def main():
    """Run all tests."""
    logger.info("=" * 60)
    logger.info("Virtual Judicial Council - Integration Test")
    logger.info(f"Started at: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    settings = get_settings()

    # Create database engine
    db_engine = create_async_engine(
        settings.get_database_url(),
        future=True,
        connect_args=settings.get_connect_args(),
    )

    try:
        # Test 1: Embedding service
        await test_embedding_service()

        # Test 2: Case parsing
        case_input = await test_case_parser()

        # Test 3: Similar case search
        similar_cases = await test_similar_case_search(db_engine, case_input)

        # Test 4: Agent deliberation
        session = await test_agent_deliberation(case_input, similar_cases)

        # Test 5: Opinion generation
        await test_opinion_generation(session)

        logger.info("\n" + "=" * 60)
        logger.info("ALL TESTS PASSED")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"\nTest failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        await db_engine.dispose()

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
