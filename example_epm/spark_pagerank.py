from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import re
import sys
from operator import add

from pyspark.sql import SparkSession

@api_endpoint
def register_de(user_id: int,
                file_name: str, ):
    """Register a DE"""
    # TODO: we check that input DE looks like lines of space separated numbers (node IDs)
    return EscrowAPI.register_de(user_id, file_name, "file", file_name, 1)


@api_endpoint
def upload_de(user_id, data_id, data_in_bytes):
    return EscrowAPI.upload_de(user_id, data_id, data_in_bytes)


@api_endpoint
def list_discoverable_des(user_id: int):
    return EscrowAPI.list_discoverable_des(user_id)


@api_endpoint
def propose_contract(user_id: int,
                     dest_agents: list[int],
                     data_elements: list[int],
                     f: str, ):
    return EscrowAPI.propose_contract(user_id, dest_agents, data_elements, f)


@api_endpoint
def show_contract(user_id: int, contract_id: int):
    return EscrowAPI.show_contract(user_id, contract_id)


@api_endpoint
def approve_contract(user_id: int, contract_id: int):
    return EscrowAPI.approve_contract(user_id, contract_id)


@api_endpoint
def execute_contract(user_id: int, contract_id: int):
    return EscrowAPI.execute_contract(user_id, contract_id)


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield url, rank / num_urls


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

@api_endpoint
@function
def calculate_page_rank():
    spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()

    exit()

    # Loads in input file.
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    # TODO: Need to use Spark Union

    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[2])):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        ))

        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))

    spark.stop()

