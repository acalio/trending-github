import re
import arrow
import pandas as pd
from src.core.datamanager.base import DataReader, DataWriter
from typing import Optional, Union, Dict, Any, List
import logging
import numpy as np
from functools import partial
import requests
import time


logger = logging.getLogger(__name__)


class GitRepoWriter(DataWriter):
    def __init__(
        self,
        *,
        filename: str,
        behaviors: Optional[
            Union[Dict[str, Any], List[Dict[str, Any]]]
        ] = None,
    ) -> None:
        super().__init__(behaviors=behaviors)
        self.filename = filename

    def write(self, data: Union[pd.DataFrame, List[pd.DataFrame]]) -> None:
        top_repo_df = data.query("source=='top_repos'")
        top_repo_per_topic_df = data.query("source=='top_repos_topic'")
        trends_df = data.query("source=='trends'")
        with pd.ExcelWriter(
            f"{arrow.now().format('YYYY-MM-DD')}_{self.filename}_report.xlsx",
            engine="openpyxl",
            mode="w",
        ) as writer:
            top_repo_df.drop(
                columns=["topic", "topics", "rn", "source", "insert_date"]
            ).to_excel(writer, sheet_name="top_repo", index=False)
            for topic, topic_df in top_repo_per_topic_df.drop(
                columns=["topics", "source", "insert_date"]
            ).groupby("topic"):
                topic_df.to_excel(writer, sheet_name=topic, index=False)
            trends_df[["full_name", "html_url", "avg"]].to_excel(
                writer, sheet_name="trends", index=False
            )


class GitRepoReader(DataReader):
    def __init__(
        self,
        *,
        url: str,
        min_stars: int,
        step_size: int,
        max_stars: int = 100000,
        behaviors: Optional[
            Union[Dict[str, Any], List[Dict[str, Any]]]
        ] = None,
    ) -> None:
        super().__init__(behaviors=behaviors)
        self.url = url
        self.min_stars = min_stars
        self.step_size = step_size
        self.max_stars = max_stars

    def set_stars(self, url: str, min_stars: int, max_stars: int) -> str:
        if "stars:>" in url or "stars:<" in url:
            url = re.sub("stars:>[0-9]+\+", "", url)
            url = re.sub("stars:<[0-9]+\+", "", url)
        if re.findall("\?q=stars:[0-9]+..[0-9]+", url):
            url = re.sub(
                "\?q=stars:[0-9]+..[0-9]+",
                f"?q=stars:{min_stars}..{max_stars}",
                url,
            )
        else:
            url = re.sub("\?q=", f"?q=stars:{min_stars}..{max_stars}+", url)
        return url

    def set_min_stars(self, url: str, stars: int) -> str:
        if "stars:>" in url:
            url = re.sub("stars:>[0-9]+", f"stars:>{stars}", url)
        else:
            url = re.sub("\?q=", f"?q=stars:>{stars}+", url)
        return url

    def set_page(self, url: str) -> str:
        if len(matches := re.findall("&page=([0-9]+)", url)) > 0:
            (page,) = map(int, matches)
            url = re.sub("&page=([0-9]+)", f"&page={page+1}", url)
        else:
            url = url + "&page=2"
        return url

    def get_urls(self):
        url = self.set_min_stars(self.url, self.max_stars)
        yield url

        for min_stars, max_stars in zip(
            range(self.min_stars, self.max_stars, self.step_size),
            range(
                self.min_stars + self.step_size,
                self.max_stars + self.step_size,
                self.step_size,
            ),
        ):
            url = self.set_stars(self.url, min_stars, max_stars)
            yield url

    def read(self) -> pd.DataFrame:
        """Legge utilizzando le search api"""
        logger.info("Start download")
        for url in self.get_urls():
            while True:
                logger.info(f"Retrieving: {url}")
                res_dict = requests.get(url).json()
                try:
                    items = res_dict["items"]
                    df = pd.DataFrame.from_dict(items)
                    if not df.empty:
                        yield df
                    # dfs.append(pd.DataFrame.from_dict(items))
                except KeyError:
                    message = res_dict.pop("message", "")
                    if "API rate limit exceeded" in message:
                        logger.warn("rate limit exceeded! I'm going to sleep")
                        time.sleep(60)
                        continue

                if not res_dict["incomplete_results"]:
                    break

                url = self.set_page(url)
                time.sleep(2)


def add_date(
    data: pd.DataFrame, col_name: str, date_format: str
) -> pd.DataFrame:
    data[col_name] = arrow.now().format(date_format)
    return data


def get_trending_topics(
    data: pd.DataFrame, col: str, new_col: str, exp: float, max_length: None
) -> pd.DataFrame:
    """Get Trending topics

    Args:
        data (pd.DataFrame):
        col (str):
        exp (float):
        max_length (None):

    Returns:
        pd.DataFrame:
    """

    def smoothing_avg(
        values: list[float], coeff: float, max_length: int = None
    ) -> float:
        size = (
            len(values) if max_length is None else min(max_length, len(values))
        )
        coefficients = np.power(coeff, np.arange(size))
        return (
            np.array(values).dot(coefficients).item()
        ) / coefficients.sum().item()

    smoothing_avg_partial = partial(
        smoothing_avg, coeff=exp, max_length=max_length
    )
    data = data.eval(f"{new_col} = {col}.apply(@smoothing_avg_partial)")
    data = data.sort_values(f"{new_col}", ascending=False)
    return data
