import json
import logging

from litellm import acompletion
from pydantic import BaseModel, Field
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
)
from tqdm import tqdm

from settings import get_settings

MODEL = "gpt-4o-mini-2024-07-18"

SUMMARIZATION_SYSTEM_PROMPT = """
You are a professional legal expert which can deeply understand the contents and
contexts of supreme court decision document
"""

SUMMARIZATION_PROMPT = """
# INSTRUCTION

Given previous processed supreme court decision summary, previous page context
and current page context from extracted supreme court decision PDF document,
generate your own understanding of the current page context and generate the concise
and corrected summary with the new important information.

# PROVIDED CONTEXTS

## CURRENT SUMMARY
This is the extracted summary from previous pages:

{current_summary}

## PREVIOUS PAGE CONTEXT
This is the context of previous page:

{previous_page_context}

## CURRENT PAGE CONTENT
This is the current page content of the court decision document from PDF, maybe
malformed due to PDF extraction noise:

{current_page_content}

# EXPECTED OUTPUT

- Ensure that any critical important information is not missing
- Ensure language used is in Bahasa Indonesia
- ONLY focus on these 4 specific informations:
    - Defendant details
    - Prosecutor's demand
    - Aggravating and mitigating circumstances
    - Supreme court final verdict ( punishment, penalty, etc..)
- Think carefully and do not mix prosecutor demand with supreme court final verdict
- Think step by step to understand the provided contexts and write a summary in the
style of professional legal expert in formalized Bahasa Indonesia.
- It MUST be properly structured in markdown format which conform COMMONMARK style
"""

TRANSLATION_SYSTEM_PROMPT = """
You are a professional legal linguistic expert which excel at translating legal decision
document summary from Bahasa Indonesia into English
"""

TRANSLATION_PROMPT = """
# INSTRUCTIONS
Below is a processed supreme court decision summary. Your task is to translate the
content into English. DO NOT modify the markdown formatting

---

{content}
"""


class CourtDecisionSummary(BaseModel):
    """
    Summary in the style of professional legal expert in Bahasa Indonesia and
    properly structured in markdown format which conform COMMONMARK style
    """

    current_page_context: str = Field(
        ...,
        description=(
            "the current page points of context and it's relation with previous page "
            "context"
        ),
    )
    improved_summary: str = Field(
        ...,
        description=(
            "the final improved supreme court document summary in markdown format"
        ),
    )


async def generate_court_decision_summary_and_translation(
    decision_number: str, doc_content: dict[int, str], max_page=int
) -> tuple[str, str]:
    logging.info(f"Starting summarization and translation for decision number: {decision_number}")
    current_summary = "No summary information yet"
    previous_page_context = "No previous page context yet"
    nrof_batch_pages = 10
    combined_content = ""

    try:
        # Incremental summarization
        logging.info(f"Beginning incremental summarization for {decision_number} - {len(doc_content)} pages")
        for page_number, content in tqdm(
            doc_content.items(), desc=f"Iterating pages for {decision_number} summary"
        ):
            logging.debug(f"Processing page {page_number} of {max_page} for {decision_number}")
            combined_content += content + "\n"

            if page_number % nrof_batch_pages == 0 or page_number == max_page:
                logging.info(f"Generating summary for batch ending at page {page_number} for {decision_number}")
                try:
                    result = await generate_summary(
                        current_page_content=combined_content,
                        previous_page_context=previous_page_context,
                        current_summary=current_summary,
                    )

                    previous_page_context = result.current_page_context
                    current_summary = result.improved_summary
                    combined_content = ""
                    logging.debug(f"Successfully generated summary batch for {decision_number} at page {page_number}")
                except Exception as e:
                    logging.error(f"Error generating summary batch for {decision_number} at page {page_number}: {str(e)}")
                    raise

        # Translation
        logging.info(f"Starting translation for {decision_number}")
        final_summary = current_summary
        try:
            translation = await generate_translation(content=final_summary)
            logging.info(f"Successfully completed summarization and translation for {decision_number}")
            return final_summary, translation
        except Exception as e:
            logging.error(f"Error during translation for {decision_number}: {str(e)}")
            raise
    except Exception as e:
        logging.error(f"Error in generate_court_decision_summary_and_translation for {decision_number}: {str(e)}")
        raise


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def generate_summary(
    current_summary: str, previous_page_context: str, current_page_content: str
) -> CourtDecisionSummary:
    logging.debug("Generating summary with API call")
    try:
        messages = [
            {"role": "system", "content": SUMMARIZATION_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": SUMMARIZATION_PROMPT.format(
                    current_summary=current_summary,
                    previous_page_context=previous_page_context,
                    current_page_content=current_page_content,
                ),
            },
        ]

        response = await acompletion(
            model=MODEL,
            messages=messages,
            response_format=CourtDecisionSummary,
            api_key=get_settings().openai_api_key,
        )

        return CourtDecisionSummary(**json.loads(response.choices[0].message.content))
    except Exception as e:
        logging.error(f"Error in generate_summary: {str(e)}")
        raise


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def generate_translation(content: str) -> str:
    logging.debug("Generating translation with API call")
    try:
        messages = [
            {"role": "system", "content": TRANSLATION_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": TRANSLATION_PROMPT.format(
                    content=content,
                ),
            },
        ]

        response = await acompletion(
            model=MODEL,
            messages=messages,
            api_key=get_settings().openai_api_key,
        )

        logging.debug("Successfully generated translation")
        return response.choices[0].message.content
    except Exception as e:
        logging.error(f"Error in generate_translation: {str(e)}")
        raise
