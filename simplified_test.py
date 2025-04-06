#!/usr/bin/env python3
"""
Simplified PDF Extraction Test Tool

This script demonstrates PDF text extraction using pdfminer.six and OCR,
along with cleaning and watermark removal. It has minimal dependencies.

Usage:
    python simplified_test.py path/to/your/file.pdf [--ocr]

Dependencies:
    - pdfminer.six
    - pytesseract and pdf2image (optional, for OCR)
"""

import sys
import re
import os
import argparse
from pathlib import Path
from io import StringIO

# Import pdfminer
try:
    from pdfminer.high_level import extract_pages, extract_text_to_fp, extract_text
    from pdfminer.layout import LAParams, LTTextContainer
except ImportError:
    print("Error: pdfminer.six not installed. Please install with:")
    print("pip install pdfminer.six")
    sys.exit(1)

# Import OCR tools (optional)
HAS_OCR = False
try:
    import pytesseract
    from pdf2image import convert_from_path
    from PIL import Image
    HAS_OCR = True
except ImportError:
    pass

def clean_text(text):
    """Clean text by removing standard headers and disclaimers."""
    text = text.replace("M a h ka m a h A g u n g R e p u blik In d o n esia\n", "")
    text = text.replace("Direktori Putusan Mahkamah Agung Republik Indonesia\n", "")
    text = text.replace("putusan.mahkamahagung.go.id\n", "")
    text = text.replace("Disclaimer\n", "")
    text = text.replace(
        "Kepaniteraan Mahkamah Agung Republik Indonesia berusaha untuk selalu mencantumkan informasi paling kini dan akurat sebagai bentuk komitmen Mahkamah Agung untuk pelayanan publik, transparansi dan akuntabilitas\n",
        "",
    )
    text = text.replace(
        "pelaksanaan fungsi peradilan. Namun dalam hal-hal tertentu masih dimungkinkan terjadi permasalahan teknis terkait dengan akurasi dan keterkinian informasi yang kami sajikan, hal mana akan terus kami perbaiki dari waktu kewaktu.\n",
        "",
    )
    text = text.replace(
        "Dalam hal Anda menemukan inakurasi informasi yang termuat pada situs ini atau informasi yang seharusnya ada, namun belum tersedia, maka harap segera hubungi Kepaniteraan Mahkamah Agung RI melalui :\n",
        "",
    )
    text = text.replace(
        "Email : kepaniteraan@mahkamahagung.go.id    Telp : 021-384 3348 (ext.318)\n",
        "",
    )
    return text

def remove_watermark(text):
    """Remove watermark patterns from text."""
    # Specific patterns to remove without being too aggressive
    patterns = [
        r'M\s*a\s*h\s*k\s*a\s*m\s*a\s*h\s*A\s*g\s*u\s*n\s*g\s*R\s*e\s*p\s*u\s*b\s*l\s*i\s*k\s*I\s*n\s*d\s*o\s*n\s*e\s*s\s*i\s*a',
        r'Mahkamah\s+Agung\s+Republik\s+Indonesia',
        r'Mahkamah\s+Agung\s+RI',
        r'Direktori\s+Putusan(\s+Mahkamah\s+Agung\s+Republik\s+Indonesia)?',
        r'putusan\.mahkamahagung\.go\.id',
        r'Disclaimer',
        r'Kepaniteraan Mahkamah Agung',
        r'Email : kepaniteraan@mahkamahagung\.go\.id',
    ]

    for pattern in patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)

    # Remove lines that only contain "Halaman X" or "Halaman X dari Y halaman"
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        trimmed = line.strip()
        # Skip lines that are just "Halaman X" or similar
        if re.match(r'^Halaman\s+\d+(\s+dari\s+\d+\s+halaman)?$', trimmed, re.IGNORECASE):
            continue
        # Skip empty lines
        if not trimmed:
            continue
        cleaned_lines.append(line)

    # Rejoin the text
    text = '\n'.join(cleaned_lines)

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)  # Replace 3+ newlines with 2
    text = re.sub(r' {2,}', ' ', text)      # Replace 2+ spaces with 1

    return text.strip()

def extract_text_with_pdfminer(pdf_path):
    """Extract text from PDF using pdfminer.six."""
    contents = {}

    try:
        # Extract text page by page using extract_text with page ranges
        from pdfminer.high_level import extract_text

        # First get total pages by trying to extract all pages
        with open(pdf_path, 'rb') as file:
            from pdfminer.pdfparser import PDFParser
            from pdfminer.pdfdocument import PDFDocument
            parser = PDFParser(file)
            document = PDFDocument(parser)
            total_pages = len(document.catalog.get('Pages').resolve().get('Kids', []))

        # Now extract each page individually
        for page_num in range(1, total_pages + 1):
            # Use the extract_text function with page_numbers parameter
            page_text = extract_text(pdf_path, page_numbers=[page_num-1])  # 0-indexed

            if page_text.strip():
                contents[page_num] = page_text

        if contents:
            print(f"Successfully extracted {len(contents)} pages using extract_text per page")
            return contents
        else:
            # If no text was extracted per page, try extracting the whole document
            text = extract_text(pdf_path)
            if text.strip():
                # Put all text in page 1 since we're falling back to whole-document extraction
                contents[1] = text
                print(f"Successfully extracted text using extract_text (whole document fallback)")
                return contents
            else:
                raise ValueError("No text extracted from PDF")

    except Exception as e:
        print(f"PDFMiner extraction failed: {str(e)}")
        raise

def extract_text_with_ocr(pdf_path, max_pages=5):
    """Extract text from PDF using OCR (scanned documents)."""
    if not HAS_OCR:
        raise ImportError("OCR libraries not installed. Please install pytesseract and pdf2image.")

    print(f"Starting OCR extraction (first {max_pages} pages)...")
    contents = {}

    try:
        # Convert PDF pages to images
        images = convert_from_path(pdf_path, first_page=1, last_page=max_pages)

        for i, image in enumerate(images):
            page_num = i + 1
            print(f"  OCR processing page {page_num}...")

            # Extract text using pytesseract - try with Indonesian language support
            try:
                text = pytesseract.image_to_string(image, lang='ind+eng')
            except:
                # Fallback to English if Indonesian not available
                text = pytesseract.image_to_string(image)

            if text.strip():
                contents[page_num] = text

        if contents:
            print(f"Successfully extracted {len(contents)} pages using OCR")
            return contents
        else:
            raise ValueError("No text extracted from PDF using OCR")

    except Exception as e:
        print(f"OCR extraction failed: {str(e)}")
        raise

def parse_args():
    parser = argparse.ArgumentParser(description="PDF text extraction tool")
    parser.add_argument("pdf_path", help="Path to the PDF file to process")
    parser.add_argument("--ocr", action="store_true", help="Use OCR for text extraction")
    parser.add_argument("--pages", type=int, default=5, help="Maximum pages to process with OCR")
    parser.add_argument("--output", "-o", help="Path to save extracted text (default: extracted_text.txt)")
    parser.add_argument("--full", action="store_true", help="Save all pages to the output file (not just the preview pages)")
    return parser.parse_args()

def save_to_file(contents, output_path, full_cleaning=True, all_pages=False):
    """Save extraction results to a text file."""
    print(f"Saving extraction results to {output_path}...")

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("===== PDF EXTRACTION RESULTS =====\n\n")

        # Determine which pages to save
        if all_pages:
            pages_to_save = sorted(contents.keys())
        else:
            # Save just the first 10 pages if not saving all
            pages_to_save = sorted(contents.keys())[:10] if len(contents) > 10 else sorted(contents.keys())

        for page_num in pages_to_save:
            original_text = contents[page_num]

            # Apply cleaning
            cleaned_text_basic = clean_text(original_text)
            if full_cleaning:
                cleaned_text = remove_watermark(cleaned_text_basic)
            else:
                cleaned_text = cleaned_text_basic

            # Write to file
            f.write(f"\n{'='*40}\n")
            f.write(f"PAGE {page_num}\n")
            f.write(f"{'='*40}\n\n")

            # Write the cleaned text to the file
            f.write(cleaned_text)
            f.write("\n\n")

        # Add summary
        f.write(f"\n{'='*40}\n")
        f.write(f"SUMMARY\n")
        f.write(f"{'='*40}\n\n")
        f.write(f"Total pages extracted: {len(contents)}\n")
        f.write(f"Pages saved to this file: {len(pages_to_save)}\n")

        if not all_pages and len(contents) > len(pages_to_save):
            f.write(f"Note: Only the first {len(pages_to_save)} pages were saved to this file.\n")
            f.write(f"Use the --full flag to save all {len(contents)} pages.\n")

    print(f"Extraction results saved to {output_path}")

def main():
    args = parse_args()
    pdf_path = args.pdf_path

    # Set default output path if not provided
    output_path = args.output if args.output else "extracted_text.txt"

    # Check if file exists
    if not os.path.exists(pdf_path):
        print(f"Error: File not found: {pdf_path}")
        return 1

    # Check if file is a PDF
    if not pdf_path.lower().endswith('.pdf'):
        print(f"Error: File is not a PDF: {pdf_path}")
        return 1

    try:
        # Extract text using chosen method
        print(f"Extracting text from {pdf_path}...")

        if args.ocr and HAS_OCR:
            print("Using OCR extraction as requested by --ocr flag")
            try:
                contents = extract_text_with_ocr(pdf_path, max_pages=args.pages)
            except Exception as e:
                print(f"OCR extraction failed: {str(e)}")
                print("Falling back to PDFMiner extraction...")
                contents = extract_text_with_pdfminer(pdf_path)
        else:
            # PDFMiner is the primary extraction method
            print("Using PDFMiner as primary extraction method...")
            try:
                contents = extract_text_with_pdfminer(pdf_path)

                # Only suggest OCR if pdfminer got very little text
                if HAS_OCR and (len(contents) == 0 or all(len(text.strip()) < 100 for text in contents.values())):
                    print("PDFMiner found minimal text. This might be a scanned document.")
                    if input("Try OCR extraction as a secondary method? (y/n): ").lower() == 'y':
                        print("Trying OCR as secondary extraction method...")
                        contents = extract_text_with_ocr(pdf_path, max_pages=args.pages)
            except Exception as e:
                if HAS_OCR:
                    print(f"PDFMiner extraction failed: {str(e)}")
                    print("Trying OCR extraction as fallback method...")
                    contents = extract_text_with_ocr(pdf_path, max_pages=args.pages)
                else:
                    raise

        # Print results with both original and cleaned text for comparison
        print("\n===== EXTRACTION RESULTS =====")

        # Process only a few pages for display
        pages_to_show = sorted(contents.keys())[:args.pages] if len(contents) > args.pages else sorted(contents.keys())

        for page_num in pages_to_show:
            original_text = contents[page_num]

            # Apply both basic cleaning and watermark removal
            cleaned_text_basic = clean_text(original_text)
            cleaned_text_full = remove_watermark(cleaned_text_basic)

            print(f"\n----- PAGE {page_num} -----")
            print("\nORIGINAL TEXT:")
            print(original_text[:300] + "..." if len(original_text) > 300 else original_text)

            print("\nCLEANED TEXT (BASIC):")
            print(cleaned_text_basic[:300] + "..." if len(cleaned_text_basic) > 300 else cleaned_text_basic)

            print("\nCLEANED TEXT (FULL WITH WATERMARK REMOVAL):")
            print(cleaned_text_full[:300] + "..." if len(cleaned_text_full) > 300 else cleaned_text_full)

            # Check if the page was completely emptied
            if (not cleaned_text_full.strip() and original_text.strip()):
                print("\n*** WARNING: Full cleaning removed all content from this page! ***")

            if (not cleaned_text_basic.strip() and original_text.strip()):
                print("\n*** WARNING: Basic cleaning removed all content from this page! ***")

        # Report on content extraction success
        empty_before = sum(1 for p in contents if not contents[p].strip())
        empty_after_basic = sum(1 for p in contents if not clean_text(contents[p]).strip())
        empty_after_full = sum(1 for p in contents if not remove_watermark(clean_text(contents[p])).strip())

        print(f"\n===== SUMMARY =====")
        print(f"Total pages extracted: {len(contents)}")
        print(f"Empty pages before cleaning: {empty_before}")
        print(f"Empty pages after basic cleaning: {empty_after_basic}")
        print(f"Empty pages after full cleaning: {empty_after_full}")

        # Ask if the user wants to save the cleaned text (avoiding watermark removal if it emptied all pages)
        use_full_cleaning = empty_after_full < len(contents)

        # Save the extraction results to the specified file
        save_to_file(contents, output_path, full_cleaning=use_full_cleaning, all_pages=args.full)

        return 0

    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())