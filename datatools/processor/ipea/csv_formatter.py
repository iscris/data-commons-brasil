import json
import logging
from traceback import format_exc
import os

import polars as pl

from datatools.processor import UTILS_PATH, LOG_HANDLER, ContextInfo, ValueFileInfo

from .common import display_name_into_stat_var_name

logger = logging.getLogger(LOG_HANDLER)


def normalize_name(name: str) -> str:
    import re
    import unicodedata

    if not name:
        return ""
    name = "".join(
        c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn"
    )
    name = re.sub(r"[^a-zA-Z0-9]", "", name.lower())
    return name


def update_data(
    csv_path: str, value_file_info: ValueFileInfo, context_info: ContextInfo
) -> pl.DataFrame | None:
    try:
        original_df = pl.read_csv(csv_path)
        if (
            "VALDATA" not in original_df.columns
            or "VALVALOR" not in original_df.columns
        ):
            logger.error("There is no VALDATA and VALVALOR in CSV")
            return None

        stat_var_name = display_name_into_stat_var_name(value_file_info.stat_var_name)

        if (
            value_file_info.has_multilevel_territories
            and value_file_info.level_info != "Country"
        ):
            level = value_file_info.level_info
            filtered_df = original_df.filter(pl.col("NIVNOME") == level).with_columns(
                pl.col("TERCODIGO").cast(pl.String, strict=False)
            )

            if level == "Estados":
                states_df = pl.read_csv(os.path.join(UTILS_PATH, "wikidata/states.csv")).with_columns(
                    pl.col("UF Code").cast(pl.String)
                )

                result = states_df.join(
                    filtered_df, left_on="UF Code", right_on="TERCODIGO", how="right"
                ).with_columns(
                    [
                        pl.col("ID").alias("wikidataId"),
                        pl.col("VALDATA").str.slice(0, 10).alias("date"),
                        pl.col("VALVALOR").alias(stat_var_name),
                    ]
                )
                final_df = result.select(["wikidataId", "date", stat_var_name])

            elif level == "Municípios":
                with open(os.path.join(UTILS_PATH, "localidades_ibge_api/municipios.json")) as f:
                    municipality_json = json.load(f)
                municipios_df = pl.DataFrame(municipality_json).with_columns(
                    pl.col("nome")
                    .map_elements(normalize_name, return_dtype=pl.String)
                    .alias("normalized_nome"),
                    pl.col("id").cast(pl.String),
                )

                filtered_df = filtered_df.join(
                    municipios_df, left_on="TERCODIGO", right_on="id", how="left"
                ).with_columns(
                    pl.col("nome")
                    .map_elements(normalize_name, return_dtype=pl.String)
                    .alias("normalized_nome")
                )

                municipality_wikidata_df = pl.read_csv(
                    os.path.join(UTILS_PATH, "wikidata/municipality.csv")
                ).with_columns(
                    pl.col("City")
                    .map_elements(normalize_name, return_dtype=pl.String)
                    .alias("normalized_City")
                )

                filtered_df = filtered_df.join(
                    municipality_wikidata_df,
                    left_on="normalized_nome",
                    right_on="normalized_City",
                    how="left",
                ).with_columns(
                    [
                        pl.col("ID").alias("wikidataId"),
                        pl.col("VALDATA").str.slice(0, 10).alias("date"),
                        pl.col("VALVALOR").alias(stat_var_name),
                    ]
                )

                final_df = filtered_df.select(["wikidataId", "date", stat_var_name])

            else:
                logger.info(f"ID mapping not concluded for level {level}")
                return None

        else:
            final_df = original_df.with_columns(
                [
                    pl.lit("BRA").alias("place"),
                    pl.col("VALDATA").str.slice(0, 10).alias("date"),
                    pl.col("VALVALOR").alias(stat_var_name),
                ]
            ).select(["place", "date", stat_var_name])

        final_df = final_df.drop_nulls()

        # Remove linhas com valores em branco (strings vazias)
        for col in final_df.columns:
            if final_df[col].dtype == pl.Utf8:
                final_df = final_df.filter(pl.col(col) != "")

        logger.debug(
            f"[csv_formatter] The file {csv_path} has stat_var_name={stat_var_name}"
        )

        return final_df

    except pl.ComputeError as cp:
        logger.error(f"[{csv_path}] {cp}")
        logger.debug(format_exc())
        return None

    except Exception as e:
        logger.error(f"[{csv_path}] {e}")
        logger.debug(format_exc())
        return None
