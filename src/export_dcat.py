"""Project-owned wrapper around the generated datannur DCAT exporter."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
GENERATED_EXPORTER = (
    REPO_ROOT / "catalog" / "app" / "scripts" / "python" / "export_dcat.py"
)


def _load_generated_module() -> Any:
    script_dir = GENERATED_EXPORTER.parent
    if str(script_dir) not in sys.path:
        sys.path.insert(0, str(script_dir))

    spec = importlib.util.spec_from_file_location(
        "datannur_generated_export_dcat", GENERATED_EXPORTER
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load generated exporter from {GENERATED_EXPORTER}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _pick_localized(
    values: dict[str, str], default_language: str, language: str, fallback: str = ""
) -> str:
    if not values:
        return fallback
    return (
        values.get(language)
        or values.get(default_language)
        or values.get("en")
        or next(iter(values.values()))
    )


def _join_values(values: list[str], language: str) -> str:
    if not values:
        return ""
    if len(values) == 1:
        return values[0]
    conjunction = " and " if language == "en" else " et " if language == "fr" else ", "
    return f"{', '.join(values[:-1])}{conjunction}{values[-1]}"


def _build_descriptions(exporter: Any, item: dict[str, Any]) -> dict[str, str]:
    descriptions = exporter._localized_fields(item, "description")
    if descriptions:
        return descriptions

    publication_types = exporter._get_publication_folder_types()
    item_type = str(item.get("type", "")).strip().lower()
    is_publication = item_type in publication_types
    distributions = (
        exporter._get_datasets_by_publication_folder().get(item.get("id"), [])
        if is_publication
        else [item]
    )
    formats = sorted(
        {
            str(dataset.get("delivery_format")).upper()
            for dataset in distributions
            if dataset.get("delivery_format")
        }
    )

    publisher = exporter.organizations.get(item.get("owner_organization_id"), {})
    publisher_names = exporter._localized_fields(publisher, "name")

    theme_labels: dict[str, list[str]] = {}
    for tag_id in exporter._split_ids(item.get("tag_ids")):
        tag = exporter.tags.get(tag_id, {})
        tag_names = exporter._localized_fields(tag, "name")
        for language in dict.fromkeys([exporter.default_language, "fr"]):
            label = _pick_localized(tag_names, exporter.default_language, language)
            if label:
                theme_labels.setdefault(language, []).append(label)

    start_date = str(item.get("start_date") or "").strip()
    end_date = str(item.get("end_date") or "").strip()
    descriptions = {}
    for language in dict.fromkeys([exporter.default_language, "fr"]):
        is_french = language == "fr"
        publisher_name = _pick_localized(
            publisher_names, exporter.default_language, language
        )
        intro = (
            "Cette publication de donnees est mise a disposition"
            if is_french and is_publication
            else "Ce jeu de donnees est mis a disposition"
            if is_french
            else "This data publication is provided"
            if is_publication
            else "This dataset is provided"
        )
        parts = [
            f"{intro} {'par' if is_french else 'by'} {publisher_name}."
            if publisher_name
            else f"{intro}."
        ]

        format_text = _join_values(formats, language)
        if format_text:
            parts.append(
                f"Formats disponibles : {format_text}."
                if is_french
                else f"Available formats: {format_text}."
            )

        theme_text = _join_values(theme_labels.get(language, []), language)
        if theme_text:
            parts.append(
                f"Themes : {theme_text}." if is_french else f"Themes: {theme_text}."
            )

        if start_date or end_date:
            temporal = (
                f"{start_date} a {end_date}"
                if start_date and end_date and is_french
                else f"{start_date} to {end_date}"
                if start_date and end_date
                else start_date or end_date
            )
            parts.append(
                f"Couverture temporelle : {temporal}."
                if is_french
                else f"Temporal coverage: {temporal}."
            )

        descriptions[language] = " ".join(parts)

    return descriptions


def _patch_module(module: Any) -> None:
    exporter_class = module.DCATExporter
    original_load_data = exporter_class.load_data

    def load_data(self: Any) -> None:
        original_load_data(self)

        for collection in (self.folders.values(), self.datasets):
            for item in collection:
                if item.get("description"):
                    continue
                descriptions = _build_descriptions(self, item)
                if not descriptions:
                    continue
                item["description"] = descriptions.get(self.default_language) or next(
                    iter(descriptions.values())
                )
                for language, description in descriptions.items():
                    if language == self.default_language:
                        continue
                    item[f"description:{language}"] = description

    exporter_class.load_data = load_data


def main() -> None:
    module = _load_generated_module()
    _patch_module(module)
    module.main()


if __name__ == "__main__":
    main()
