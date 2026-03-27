#!/usr/bin/env python3
"""
MUTCD to Pinecone Indexer
Uploads Virginia MUTCD JSON content to Pinecone for RAG-based search.

Usage:
    python index_mutcd_to_pinecone.py

Requirements:
    pip install pinecone
"""

import json
import os
import time
from pathlib import Path

try:
    from pinecone import Pinecone
except ImportError:
    print("Error: pinecone package not installed.")
    print("Run: pip install pinecone")
    exit(1)

# Configuration - Set via environment variables or edit here
PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY", "pcsk_2nM7Kz_N4J6XTqVyPS1XwHdR6NbzhB6HPGKpxTsWzw75otHQygEzRbdTfKYvPUhBYRCNW4")
PINECONE_INDEX_HOST = os.environ.get("PINECONE_HOST", "https://va-mutcd-h0lxejj.svc.aped-4627-b74a.pinecone.io")
INDEX_NAME = "va-mutcd"
EMBEDDING_MODEL = "llama-text-embed-v2"  # Pinecone's built-in model

# Path to MUTCD JSON files
SCRIPT_DIR = Path(__file__).parent
MUTCD_DIR = SCRIPT_DIR.parent / "data" / "va_mutcd"

def load_mutcd_sections():
    """Load all MUTCD sections from JSON files."""
    sections = []

    # Get all part JSON files
    part_files = sorted(MUTCD_DIR.glob("part*.json"))

    print(f"Found {len(part_files)} MUTCD part files")

    for part_file in part_files:
        print(f"  Loading: {part_file.name}")

        with open(part_file, 'r', encoding='utf-8') as f:
            part_data = json.load(f)

        part_id = part_data.get('part_id', '')
        part_number = part_data.get('part_number', 0)
        part_title = part_data.get('title', '')

        # Extract sections from each chapter
        for chapter in part_data.get('chapters', []):
            chapter_code = chapter.get('chapter_code', '')
            chapter_title = chapter.get('title', '')
            chapter_keywords = chapter.get('keywords', [])

            for section in chapter.get('sections', []):
                section_number = section.get('section_number', '')
                section_title = section.get('title', '')
                virginia_specific = section.get('virginia_specific', False)
                content = section.get('content', {})
                raw_text = section.get('raw_text', '')

                # Build searchable text combining all content types
                text_parts = []

                # Add section header
                text_parts.append(f"Section {section_number}: {section_title}")
                text_parts.append(f"Chapter {chapter_code}: {chapter_title}")
                text_parts.append(f"Part {part_number}: {part_title}")

                if virginia_specific:
                    text_parts.append("(Virginia-specific content)")

                # Add content by type with labels
                for content_type in ['standard', 'guidance', 'option', 'support']:
                    items = content.get(content_type, [])
                    if items:
                        label = {
                            'standard': 'STANDARD (SHALL)',
                            'guidance': 'GUIDANCE (SHOULD)',
                            'option': 'OPTION (MAY)',
                            'support': 'SUPPORT'
                        }.get(content_type, content_type.upper())

                        for item in items:
                            if item.strip():
                                text_parts.append(f"{label}: {item}")

                # Combine into single searchable text
                full_text = "\n".join(text_parts)

                # Skip empty sections
                if len(full_text.strip()) < 50:
                    continue

                # Truncate very long texts (Pinecone has limits)
                if len(full_text) > 8000:
                    full_text = full_text[:8000] + "..."

                sections.append({
                    'id': f"mutcd_{section_number.replace('.', '_')}",
                    'text': full_text,
                    'metadata': {
                        'section_number': section_number,
                        'section_title': section_title,
                        'chapter_code': chapter_code,
                        'chapter_title': chapter_title,
                        'part_number': part_number,
                        'part_title': part_title,
                        'virginia_specific': virginia_specific,
                        'keywords': ','.join(chapter_keywords[:10]),  # Limit keywords
                        'content_types': ','.join([k for k in ['standard', 'guidance', 'option', 'support'] if content.get(k)]),
                        # Include actual content (truncated to fit Pinecone metadata limits)
                        'content_text': full_text[:30000] if len(full_text) <= 30000 else full_text[:30000] + '...[truncated]'
                    }
                })

    return sections


def create_embeddings_and_upsert(pc, index, sections, batch_size=50):
    """Create embeddings using Pinecone Inference and upsert to index."""

    total = len(sections)
    print(f"\nIndexing {total} sections to Pinecone...")

    for i in range(0, total, batch_size):
        batch = sections[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total + batch_size - 1) // batch_size

        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} sections)...")

        # Extract texts for embedding
        texts = [s['text'] for s in batch]

        try:
            # Use Pinecone Inference API to create embeddings
            embeddings_response = pc.inference.embed(
                model=EMBEDDING_MODEL,
                inputs=texts,
                parameters={"input_type": "passage"}
            )

            # Prepare vectors for upsert
            vectors = []
            for j, section in enumerate(batch):
                vectors.append({
                    'id': section['id'],
                    'values': embeddings_response.data[j].values,
                    'metadata': section['metadata']
                })

            # Upsert to index
            index.upsert(vectors=vectors)

            print(f"    Uploaded {len(vectors)} vectors")

        except Exception as e:
            print(f"    Error in batch {batch_num}: {e}")
            # Try smaller batches on error
            if batch_size > 10:
                print("    Retrying with smaller batch size...")
                for section in batch:
                    try:
                        emb = pc.inference.embed(
                            model=EMBEDDING_MODEL,
                            inputs=[section['text']],
                            parameters={"input_type": "passage"}
                        )
                        index.upsert(vectors=[{
                            'id': section['id'],
                            'values': emb.data[0].values,
                            'metadata': section['metadata']
                        }])
                    except Exception as e2:
                        print(f"      Failed: {section['id']} - {e2}")

        # Rate limiting
        time.sleep(0.5)

    print("\nIndexing complete!")


def main():
    print("=" * 60)
    print("MUTCD to Pinecone Indexer")
    print("=" * 60)

    # Initialize Pinecone
    print("\nConnecting to Pinecone...")
    pc = Pinecone(api_key=PINECONE_API_KEY)

    # Connect to index
    index = pc.Index(
        name=INDEX_NAME,
        host=PINECONE_INDEX_HOST
    )

    # Check index stats
    stats = index.describe_index_stats()
    print(f"Current index stats: {stats.total_vector_count} vectors")

    if stats.total_vector_count > 0:
        response = input("\nIndex already has data. Clear and re-index? (y/n): ")
        if response.lower() == 'y':
            print("Clearing existing vectors...")
            index.delete(delete_all=True)
            time.sleep(2)
        else:
            print("Keeping existing data. New sections will be added/updated.")

    # Load MUTCD sections
    print("\nLoading MUTCD JSON files...")
    sections = load_mutcd_sections()
    print(f"Loaded {len(sections)} sections")

    # Create embeddings and upload
    create_embeddings_and_upsert(pc, index, sections)

    # Final stats
    time.sleep(2)
    stats = index.describe_index_stats()
    print(f"\nFinal index stats: {stats.total_vector_count} vectors")
    print("\nDone! Your MUTCD content is now searchable via Pinecone.")


if __name__ == "__main__":
    main()
