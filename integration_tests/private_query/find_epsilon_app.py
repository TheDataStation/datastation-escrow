import random

random_state = 0
random.seed(random_state)

import itertools
import math
import numbers
import re

import numpy as np
import pandas as pd
# from scipy import stats
import tqdm
import time
import warnings
import snsql
from snsql import Privacy
from snsql._ast.expressions.date import parse_datetime
from snsql._ast.expressions import sql as ast
from snsql.sql.reader.base import SortKey
from snsql._ast.ast import Top
from sqlalchemy import create_engine
from pandas.io.sql import to_sql, read_sql
# import matplotlib.pyplot as plt
from statsmodels.stats.multitest import fdrcorrection
from sql_metadata import Parser
# from sklearn import preprocessing
# import multiprocessing as mp
# from multiprocessing.pool import Pool
import pathos.multiprocessing as mp
# from pathos.multiprocessing import ProcessPool
from collections import defaultdict
# from scipy.stats import laplace
# from scipy import spatial

from dsapplicationregistration import register
from common import utils
import pathlib

def get_metadata(df: pd.DataFrame, name: str):
    metadata = {}
    metadata[name] = {}  # collection
    metadata[name][name] = {}  # db
    metadata[name][name][name] = {}  # table

    t = metadata[name][name][name]
    t["row_privacy"] = True
    t["rows"] = len(df)

    df_inferred = df.convert_dtypes()
    # print(df_inferred.dtypes)

    for col in df_inferred:
        t[str(col)] = {}
        c = t[str(col)]
        if pd.api.types.is_string_dtype(df_inferred[col]):
            c["type"] = "string"
        elif pd.api.types.is_bool_dtype(df_inferred[col]):
            c["type"] = "boolean"
        elif pd.api.types.is_datetime64_any_dtype(df_inferred[col]):
            c["type"] = "datetime"
        # elif pd.api.types.is_numeric_dtype(df_inferred[col]):
        #     if (df[col].fillna(-9999) % 1  == 0).all():
        #         c["type"] = "int"
        #     else:
        #         c["type"] = "float"
        elif pd.api.types.is_integer_dtype(df_inferred[col]) or pd.api.types.is_timedelta64_dtype(df_inferred[col]):
            c["type"] = "int"
            c["upper"] = str(df_inferred[col].max())
            c["lower"] = str(df_inferred[col].min())
        elif pd.api.types.is_float_dtype(df_inferred[col]):
            c["type"] = "float"
            c["upper"] = str(df_inferred[col].max())
            c["lower"] = str(df_inferred[col].min())
        else:
            raise ValueError("Unknown column type for column {0}".format(col))

    return metadata


def fdr(p_values, q):
    rejected, pvalue_corrected = fdrcorrection(p_values, alpha=q)
    # print(any(rejected))
    return any(rejected)

    # # Based on https://matthew-brett.github.io/teaching/fdr.html
    #
    # # q = 0.1 # proportion of false positives we will accept
    # N = len(p_values)
    # sorted_p_values = np.sort(p_values)  # sort p-values ascended
    # i = np.arange(1, N + 1)  # the 1-based i index of the p values, as in p(i)
    # below = (sorted_p_values < (q * i / N))  # True where p(i)<q*i/N
    # if len(np.where(below)[0]) == 0:
    #     return False
    # max_below = np.max(np.where(below)[0])  # max index where p(i)<q*i/N
    # print('p*:', sorted_p_values[max_below])
    #
    # # num_discoveries = 0
    # for p in p_values:
    #     if p < sorted_p_values[max_below]:
    #         print("Discovery: " + str(p))
    #         return True
    #         # num_discoveries += 1
    # # print("num_discoveries =", num_discoveries)
    # return False


def compute_neighboring_results(df, query_string, idx_to_compute, table_name, row_num_col, table_metadata):
    neighboring_results = []
    # original_risks = []
    # risk_score_cache = {}

    query_string = re.sub(f" WHERE ", f" WHERE ", query_string, flags=re.IGNORECASE)
    query_string = re.sub(f" FROM ", f" FROM ", query_string, flags=re.IGNORECASE)

    where_pos = query_string.rfind(" WHERE ")  # last pos of WHERE
    table_name_pos = None
    # no_where_clause = False
    if where_pos == -1:
        # no_where_clause = True
        table_name_pos = query_string.rfind(f" FROM {table_name}")
        table_name_pos += len(f" FROM {table_name}")
    else:
        where_pos += len(" WHERE ")

    engine = create_engine("sqlite:///:memory:")
    # engine = create_engine(f"sqlite:///file:memdb{num}?mode=memory&cache=shared&uri=true")

    with engine.connect() as conn:

        # start_time = time.time()

        # dummy_row = df.iloc[0].copy()
        # for col in df.columns:
        #     if col in table_metadata.keys():
        #         if table_metadata[col]["type"] == "int" or table_metadata[col]["type"] == "float":
        #             dummy_row[col] = table_metadata[col]["lower"]
        #         elif table_metadata[col]["type"] == "string":
        #             dummy_row[col] = None
        #     else:
        #         dummy_row[col] = len(df) + 1
        #
        # # print(dummy_row)
        # # df_with_dummy_row = df.append(dummy_row)
        # df_with_dummy_row = pd.concat([df, pd.DataFrame([dummy_row])], ignore_index=True)

        num_rows = to_sql(df, name=table_name, con=conn,
                          # index=not any(name is None for name in df.index.names),
                          if_exists="replace")  # load index into db if all levels are named
        if num_rows != len(df):
            print("error when loading to sqlite")
            return None

        # insert_db_time = time.time() - start_time
        # print(f"time to insert to db: {insert_db_time} s")

        for i in tqdm.tqdm(idx_to_compute):

            if i == -1:
                neighboring_results.append(None)
                continue

            # column_values = tuple(df.iloc[i][query_columns].values)
            # print(column_values)
            # if column_values not in risk_score_cache.keys():

            start_time = time.time()

            if where_pos == -1:
                cur_query = query_string[:table_name_pos] + f" WHERE {row_num_col} != {i} " + query_string[
                                                                                              table_name_pos:]
            else:
                cur_query = query_string[:where_pos] + f"{row_num_col} != {i} AND " + query_string[where_pos:]

            # print(cur_query)
            cur_result = read_sql(sql=cur_query, con=conn)
            # print(cur_result.sum(numeric_only=True).sum())

            # risk_score = abs(
            #     cur_result.sum(numeric_only=True).sum() - original_result.sum(numeric_only=True).sum())

            elapsed = time.time() - start_time
            # print(f"time to execute one query: {elapsed} s")
            # break

            # risk_score_cache[column_values] = risk_score
            # else:
            #     risk_score = risk_score_cache[column_values]

            neighboring_results.append(cur_result)

    # engine.dispose()
    # original_risks_dict[num] = original_risks
    # mp_queue.put((num, original_risks))

    return neighboring_results


def execute_rewritten_ast(sqlite_connection, table_name,
                          private_reader, subquery, query, *ignore, accuracy: bool = False, pre_aggregated=None,
                          postprocess=True):
    private_reader._options.censor_dims = False
    private_reader._options.clamp_counts = True

    # if isinstance(query, str):
    #     raise ValueError("Please pass AST to _execute_ast.")
    #
    # subquery, query = self._rewrite_ast(query)

    if pre_aggregated is not None:
        exact_aggregates = private_reader._check_pre_aggregated_columns(pre_aggregated, subquery)
    else:
        # exact_aggregates = private_reader._get_reader(subquery)._execute_ast(subquery)
        reader = private_reader._get_reader(subquery)
        # if isinstance(query, str):
        #     raise ValueError("Please pass ASTs to execute_ast.  To execute strings, use execute.")
        if hasattr(reader, "serializer") and reader.serializer is not None:
            query_string = reader.serializer.serialize(subquery)
        else:
            query_string = str(subquery)
        # exact_aggregates = reader.execute(query_string, accuracy=accuracy)

        # print(read_sql(sql=f"SELECT * FROM {table_name}", con=sqlite_connection))

        # print(query_string)
        query_string = re.sub(f" FROM {table_name}.{table_name}", f" FROM {table_name}", query_string,
                              flags=re.IGNORECASE)

        # print(query_string)
        q_result = read_sql(sql=query_string, con=sqlite_connection)
        # print(q_result)
        exact_aggregates = [tuple([col for col in q_result.columns])] + [val[1:] for val in q_result.itertuples()]
        # print(exact_aggregates)

    _accuracy = None
    if accuracy:
        raise NotImplementedError(
            "Simple accuracy has been removed.  Please see documentation for information on estimating accuracy.")

    syms = subquery._select_symbols
    source_col_names = [s.name for s in syms]

    # tell which are counts, in column order
    is_count = [s.expression.is_count for s in syms]

    # get a list of mechanisms in column order
    mechs = private_reader._get_mechanisms(subquery)
    check_sens = [m for m in mechs if m]
    if any([m.sensitivity is np.inf for m in check_sens]):
        raise ValueError(f"Attempting to query an unbounded column")

    kc_pos = private_reader._get_keycount_position(subquery)

    def randomize_row_values(row_in):
        row = [v for v in row_in]
        # set null to 0 before adding noise
        for idx in range(len(row)):
            if mechs[idx] and row[idx] is None:
                row[idx] = 0.0
        # call all mechanisms to add noise
        return [
            mech.release([v])[0] if mech is not None else v
            for mech, v in zip(mechs, row)
        ]

    if hasattr(exact_aggregates, "rdd"):
        # it's a dataframe
        out = exact_aggregates.rdd.map(randomize_row_values)
    elif hasattr(exact_aggregates, "map"):
        # it's an RDD
        out = exact_aggregates.map(randomize_row_values)
    elif isinstance(exact_aggregates, list):
        out = map(randomize_row_values, exact_aggregates[1:])
    elif isinstance(exact_aggregates, np.ndarray):
        out = map(randomize_row_values, exact_aggregates)
    else:
        raise ValueError("Unexpected type for exact_aggregates")

    # print(list(out))

    # censor infrequent dimensions
    if private_reader._options.censor_dims:
        if kc_pos is None:
            raise ValueError("Query needs a key count column to censor dimensions")
        else:
            thresh_mech = mechs[kc_pos]
            private_reader.tau = thresh_mech.threshold
            # print("xxx")
        if hasattr(out, "filter"):
            # it's an RDD
            tau = private_reader.tau
            out = out.filter(lambda row: row[kc_pos] > tau)
            # print("yyy")
        else:
            # print(kc_pos)
            # print(private_reader.tau)
            out = filter(lambda row: row[kc_pos] > private_reader.tau, out)
            # print("zzz")

    # print(list(out))

    if not postprocess:
        return out

    # print(list(out))

    def process_clamp_counts(row_in):
        # clamp counts to be non-negative
        row = [v for v in row_in]
        for idx in range(len(row)):
            if is_count[idx] and row[idx] < 0:
                row[idx] = 0
        return row

    clamp_counts = private_reader._options.clamp_counts
    # print(clamp_counts)
    if clamp_counts:
        if hasattr(out, "rdd"):
            # it's a dataframe
            out = out.rdd.map(process_clamp_counts)
        elif hasattr(out, "map"):
            # it's an RDD
            out = out.map(process_clamp_counts)
        else:
            out = map(process_clamp_counts, out)

    # get column information for outer query
    out_syms = query._select_symbols
    out_types = [s.expression.type() for s in out_syms]
    out_col_names = [s.name for s in out_syms]

    def convert(val, type):
        if val is None:
            return None  # all columns are nullable
        if type == "string" or type == "unknown":
            return str(val)
        elif type == "int":
            return int(float(str(val).replace('"', "").replace("'", "")))
        elif type == "float":
            return float(str(val).replace('"', "").replace("'", ""))
        elif type == "boolean":
            if isinstance(val, int):
                return val != 0
            else:
                return bool(str(val).replace('"', "").replace("'", ""))
        elif type == "datetime":
            v = parse_datetime(val)
            if v is None:
                raise ValueError(f"Could not parse datetime: {val}")
            return v
        else:
            raise ValueError("Can't convert type " + type)

    alphas = [alpha for alpha in private_reader.privacy.alphas]

    def process_out_row(row):
        bindings = dict((name.lower(), val) for name, val in zip(source_col_names, row))
        out_row = [c.expression.evaluate(bindings) for c in query.select.namedExpressions]
        try:
            out_row = [convert(val, type) for val, type in zip(out_row, out_types)]
        except Exception as e:
            raise ValueError(
                f"Error converting output row: {e}\n"
                f"Expecting types {out_types}"
            )

        # compute accuracies
        if accuracy == True and alphas:
            accuracies = [_accuracy.accuracy(row=list(row), alpha=alpha) for alpha in alphas]
            return tuple([out_row, accuracies])
        else:
            return tuple([out_row, []])

    if hasattr(out, "map"):
        # it's an RDD
        out = out.map(process_out_row)
    else:
        out = map(process_out_row, out)

    def filter_aggregate(row, condition):
        bindings = dict((name.lower(), val) for name, val in zip(out_col_names, row[0]))
        keep = condition.evaluate(bindings)
        return keep

    if query.having is not None:
        condition = query.having.condition
        if hasattr(out, "filter"):
            # it's an RDD
            out = out.filter(lambda row: filter_aggregate(row, condition))
        else:
            out = filter(lambda row: filter_aggregate(row, condition), out)

    # sort it if necessary
    if query.order is not None:
        sort_fields = []
        for si in query.order.sortItems:
            if type(si.expression) is not ast.Column:
                raise ValueError("We only know how to sort by column names right now")
            colname = si.expression.name.lower()
            if colname not in out_col_names:
                raise ValueError(
                    "Can't sort by {0}, because it's not in output columns: {1}".format(
                        colname, out_col_names
                    )
                )
            colidx = out_col_names.index(colname)
            desc = False
            if si.order is not None and si.order.lower() == "desc":
                desc = True
            if desc and not (out_types[colidx] in ["int", "float", "boolean", "datetime"]):
                raise ValueError("We don't know how to sort descending by " + out_types[colidx])
            sf = (desc, colidx)
            sort_fields.append(sf)

        def sort_func(row):
            # use index 0, since index 1 is accuracy
            return SortKey(row[0], sort_fields)

        if hasattr(out, "sortBy"):
            out = out.sortBy(sort_func)
        else:
            out = sorted(out, key=sort_func)

    # check for LIMIT or TOP
    limit_rows = None
    if query.limit is not None:
        if query.select.quantifier is not None:
            raise ValueError("Query cannot have both LIMIT and TOP set")
        limit_rows = query.limit.n
    elif query.select.quantifier is not None and isinstance(query.select.quantifier, Top):
        limit_rows = query.select.quantifier.n
    if limit_rows is not None:
        if hasattr(out, "rdd"):
            # it's a dataframe
            out = out.limit(limit_rows)
        elif hasattr(out, "map"):
            # it's an RDD
            out = out.take(limit_rows)
        else:
            out = itertools.islice(out, limit_rows)

    # drop empty accuracy if no accuracy requested
    def drop_accuracy(row):
        return row[0]

    if accuracy == False:
        if hasattr(out, "rdd"):
            # it's a dataframe
            out = out.rdd.map(drop_accuracy)
        elif hasattr(out, "map"):
            # it's an RDD
            out = out.map(drop_accuracy)
        else:
            out = map(drop_accuracy, out)

    # print(list(out))

    # increment odometer
    for mech in mechs:
        if mech:
            private_reader.odometer.spend(Privacy(epsilon=mech.epsilon, delta=mech.delta))

    # output it
    if accuracy == False and hasattr(out, "toDF"):
        # Pipeline RDD
        if not out.isEmpty():
            return out.toDF(out_col_names)
        else:
            return out
    elif hasattr(out, "map"):
        # Bare RDD
        return out
    else:
        row0 = [out_col_names]
        if accuracy == True:
            row0 = [[out_col_names,
                     [[col_name + '_' + str(1 - alpha).replace('0.', '') for col_name in out_col_names] for alpha in
                      private_reader.privacy.alphas]]]
        out_rows = row0 + list(out)
        return out_rows


def extract_table_names(query):
    """ Extract table names from an SQL query. """
    # a good old fashioned regex. turns out this worked better than actually parsing the code
    tables_blocks = re.findall(r'(?:FROM|JOIN)\s+(\w+(?:\s*,\s*\w+)*)', query, re.IGNORECASE)
    tables = [tbl
              for block in tables_blocks
              for tbl in re.findall(r'\w+', block)]
    return set(tables)


@register()
def find_epsilon(context,
                 file_to_query: str,
                 query_string: str,
                 eps_to_test: list,
                 percentile: int,
                 threshold: float = 0.01,
                 num_parallel_processes: int = 8):

    files = utils.get_all_files()
    # print(files)
    if file_to_query not in files:
        print(f"{file_to_query} not accessible")
        return None

    df = pd.read_csv(file_to_query)

    with warnings.catch_warnings():
        warnings.simplefilter(action="ignore")

        table_names = extract_table_names(query_string)
        if len(table_names) != 1:
            print("error: can only query on one table")
            return None
        if query_string.count("SELECT ") > 1:
            # there's subquery
            print("error: can't have subquery")
            return None
        table_name = table_names.pop()
        # print(table_name)

        sql_parser = Parser(query_string)
        query_columns = sql_parser.columns
        # print(query_columns)
        df_copy = df.copy()
        df_copy = df_copy[query_columns]
        # df_copy = df_copy.sample(frac=1, random_state=random_state).reset_index(drop=True)

        metadata = get_metadata(df_copy, table_name)
        table_metadata = metadata[table_name][table_name][table_name]

        # just needed to create a row_num col that doesn't already exist
        row_num_col = f"row_num_{random.randint(0, 10000)}"
        while row_num_col in df_copy.columns:
            row_num_col = f"row_num_{random.randint(0, 10000)}"
        df_copy[row_num_col] = df_copy.reset_index().index
        df_copy.set_index(row_num_col)

        start_time = time.time()

        engine = create_engine("sqlite:///:memory:")
        sqlite_connection = engine.connect()

        num_rows = to_sql(df_copy, name=table_name, con=sqlite_connection,
                          # index=not any(name is None for name in df_copy.index.names),
                          if_exists="replace")  # load index into db if all levels are named
        if num_rows != len(df_copy):
            print("error when loading to sqlite")
            return None

        insert_db_time = time.time() - start_time
        # print(f"time to insert to db: {insert_db_time} s")

        start_time = time.time()

        original_result = read_sql(sql=query_string, con=sqlite_connection)
        original_aggregates = [val[1:] for val in original_result.itertuples()]

        # if len(original_result.select_dtypes(include=np.number).columns) > 1:
        #     print("error: can only have one numerical column in the query result")
        #     return None

        # print("original_result", original_aggregates)
        # print(original_result)

        neighboring_results = []

        # start_time = time.time()

        cache = defaultdict(list)

        columns_values = list(df[query_columns].itertuples(index=False, name=None))

        for i in range(len(columns_values)):
            cache[columns_values[i]].append(i)

        inv_cache = {}
        indices = np.arange(len(df_copy))
        indices_to_ignore = []

        for k, v in cache.items():

            indices_to_ignore += v[1:]  # only need to compute result for one

            for i in v:
                inv_cache[i] = k

        np.put(indices, indices_to_ignore, [-1] * len(indices_to_ignore))

        # print(len(indices), len(indices_to_ignore), len(indices) - len(indices_to_ignore))

        elapsed = time.time() - start_time
        # print(f"time to create cache: {elapsed} s")

        start_time = time.time()

        idx_split = np.array_split(indices, num_parallel_processes)

        with mp.Pool(processes=num_parallel_processes) as mp_pool:

            args = [(df_copy, query_string, idx_to_compute, table_name, row_num_col, table_metadata)
                    for idx_to_compute in idx_split]

            for cur_neighboring_results in mp_pool.starmap(compute_neighboring_results, args):  #
                # *np.array(args).T):
                neighboring_results += cur_neighboring_results

            # compute_exact_aggregates_of_neighboring_data(df_copy, table_name, row_num_col, idx_to_compute,
            #                                              private_reader, subquery, query)

        neighboring_aggregates = []

        for i in range(len(neighboring_results)):
            if neighboring_results[i] is None:
                idx_computed = cache[inv_cache[i]][0]
                neighboring_results[i] = neighboring_results[idx_computed]

            neighboring_result = neighboring_results[i]
            neighboring_aggregate = [val[1:] for val in neighboring_result.itertuples()]
            # print("neighboring_aggregates", neighboring_aggregates)

            if len(neighboring_aggregate) != len(original_aggregates):
                # corner case: group by results missing for one group after removing one record
                # print("in")
                missing_group = set(original_aggregates) - set(neighboring_aggregate)
                # assert len(missing_group) == 1
                missing_group = missing_group.pop()
                missing_group_pos = original_aggregates.index(missing_group)
                missing_group = list(missing_group)
                for i in range(len(missing_group)):
                    if isinstance(missing_group[i], numbers.Number):
                        # print("xxxx")
                        missing_group[i] = 0
                neighboring_aggregate.insert(missing_group_pos, tuple(missing_group))

            # print("neighboring_aggregates", neighboring_aggregates)

            neighboring_aggregates.append(neighboring_aggregate)

        elapsed = time.time() - start_time
        # print(f"time to compute neighboring_results: {elapsed} s")

        best_eps = None

        # We start from the largest epsilon to the smallest
        sorted_eps_to_test = np.sort(eps_to_test)[::-1]

        compute_risk_time = 0.0
        test_equal_distribution_time = 0.0

        # query_string = query_string.replace(f" {table_name} ", f" {table_name}.{table_name} ")
        query_string = re.sub(f" FROM {table_name}", f" FROM {table_name}.{table_name}", query_string,
                              flags=re.IGNORECASE)

        for eps in sorted_eps_to_test:

            print(f"epsilon = {eps}")

            p_values = []

            privacy = Privacy(epsilon=eps)
            private_reader = snsql.from_df(df_copy, metadata=metadata, privacy=privacy)
            # private_reader._options.censor_dims = False
            # private_reader.rewriter.options.censor_dims = False
            # dp_result = private_reader.execute_df(query)
            # rewrite the query ast once for every epsilon
            query_ast = private_reader.parse_query_string(query_string)
            try:
                subquery, query = private_reader._rewrite_ast(query_ast)
            except ValueError as err:
                print(err)
                return None
            # col_sensitivities = get_query_sensitivities(private_reader, subquery, query)
            # print(col_sensitivities)

            start_time = time.time()

            dp_result = execute_rewritten_ast(sqlite_connection, table_name, private_reader, subquery, query)
            dp_result = private_reader._to_df(dp_result)
            # print(dp_result)
            dp_aggregates = [val[1:] for val in dp_result.itertuples()]

            # print("dp_result", dp_result)
            # print("dp_aggregates", dp_aggregates)

            PRIs = []
            for neighboring_aggregate in neighboring_aggregates:

                PRI = 0
                for row1, row2 in zip(dp_aggregates, neighboring_aggregate):
                    for val1, val2 in zip(row1, row2):
                        if isinstance(val1, numbers.Number) and isinstance(val2, numbers.Number):
                            PRI += abs(val1 - val2)

                # if eps == 0.1:
                #     print(neighboring_aggregates, PRI)

                PRIs.append(PRI)

            '''

            PRIs.sort()

            # l2 normalization
            # PRIs = preprocessing.normalize([PRIs])[0]

            # min-min normalize
            # PRIs = preprocessing.minmax_scale(np.array(PRIs).reshape(-1, 1)).reshape(-1)

            # standardize
            # PRIs = np.array(PRIs)
            # PRIs = (PRIs - PRIs.mean()) / PRIs.std()

            # print(PRIs)
            print(pd.DataFrame(PRIs).describe())
            # print(PRIs)

            # PRIs = np.unique(PRIs)

            p1 = np.percentile(PRIs, 100 - risk_group_percentile)
            PRI1 = [val for val in PRIs if val >= p1]

            p2 = np.percentile(PRIs, risk_group_percentile)
            PRI2 = [val for val in PRIs if val <= p2]

            PRI1 = PRIs[:100]
            PRI2 = PRIs[-100:]

            # random_sample = np.random.choice(PRIs, size=1000, replace=False)
            PRIs_shuffled = PRIs.copy()
            random.shuffle(PRIs_shuffled)
            sample_split = np.array_split(PRIs_shuffled, 10)
            # print(*sample_split)

            # print(f"{100 - risk_group_percentile} percentile", p1, f"{risk_group_percentile} percentile", p2)
            print(PRI1)
            print(PRI2)

            eucl_dist = np.linalg.norm(np.array(PRI2) - np.array(PRI1))
            print("eucl dist", eucl_dist)

            cos_dist = 1 - spatial.distance.cosine(PRI1, PRI2)
            print("cos dist", cos_dist)

            emd_dist = stats.wasserstein_distance(PRI1, PRI2)
            print("emd dist", emd_dist)

            min_dist = spatial.distance.minkowski(PRI1, PRI2)
            print("min dist", min_dist)

            elapsed = time.time() - start_time
            # print(f"{j}th compute risk time: {elapsed} s")
            compute_risk_time += elapsed

            # We perform the test and record the p-value for each run
            start_time = time.time()

            if test == "mw":
                cur_res = stats.mannwhitneyu(PRI1, PRI2, method="asymptotic")
                p_value = cur_res[1]
            elif test == "ks":
                cur_res = stats.ks_2samp(PRI1, PRI2)#, method="asymp")  # , method="exact")
                p_value = cur_res[1]
            elif test == "es":
                for i1 in range(len(PRI1)):
                    if PRI1[i1] == 0.0:
                        PRI1[i1] += 1e-100
                cur_res = stats.epps_singleton_2samp(PRI1, PRI2)
                p_value = cur_res[1]
            elif test == "ad":
                cur_res = stats.anderson_ksamp(sample_split)
                p_value = cur_res[2]
            elif test == "mood":
                cur_res = stats.median_test(PRI1, PRI2)
                p_value = cur_res[1]
            elif test == "kw":
                cur_res = stats.kruskal(*sample_split)
                p_value = cur_res[1]

            print("res", cur_res)
            # p_value = cur_res[1]
            # if test == "ad":
            #     p_value = cur_res[2]
            # if p_value < 0.01:
            #     reject_null = True # early stopping
            p_values.append(p_value)

            # print(pd.DataFrame(new_risks1).describe())
            # print(pd.DataFrame(new_risks2).describe())
            # print(f"p_value: {p_value}")

            elapsed = time.time() - start_time
            test_equal_distribution_time += elapsed

        # Now we have n p-values for the current epsilon, we use multiple comparisons' technique
        # to determine if this epsilon is good enough
        # For now we use false discovery rate
        # print(p_values)
        if test_eps_num_runs > 1:
            if not fdr(p_values, q):  # q = proportion of false positives we will accept
                # We want no discovery (fail to reject null) for all n runs
                # If we fail to reject the null, then we break the loop.
                # The current epsilon is the one we choose
                best_eps = eps
                break
        else:
            if p_values[0] > q:
                best_eps = eps
                # break

        # TODO: if we find a lot of discoveries (a lot of small p-values),
        #  we can skip epsilons that are close to the current eps (ex. 10 to 9).
        #  If there's only a few discoveries,
        #  we should probe epsilons that are close (10 to 9.9)

        '''
            # print(pd.DataFrame(PRIs).describe())

            # min = np.min(PRIs)
            max = np.max(PRIs)
            # median = np.median(PRIs)
            denom = np.percentile(PRIs, percentile)

            # print("max / min", max / min)
            # print("max / 25p", max / p25)
            # print("max / med", max / median)

            if max / denom < 1.0 + threshold:
                best_eps = eps
                break

        # print(f"total time to compute new risk: {compute_risk_time} s")
        # print(f"total time to test equal distribution: {test_equal_distrbution_time} s")

        sqlite_connection.close()

        if best_eps is None:
            return None

        return best_eps, dp_result, insert_db_time  # also return the dp result computed


@register()
def read_catalog(context, catalog_id=None):
    # return the schema information for each csv file in storage (that are accessible)

    files = utils.get_all_files()
    schema_dict = {}
    for file in files:
        if pathlib.Path(file).suffix == ".csv":
            csv_path = pathlib.Path(file).absolute()
            df = pd.read_csv(csv_path)
            schema = df.columns.values.tolist()
            schema_dict[file] = schema

    return schema_dict

if __name__ == '__main__':
    # csv_path = '../adult.csv'
    # df = pd.read_csv(csv_path)#.head(100)
    # print(df.head())
    df = pd.read_csv("../scalability/adult_1000.csv")
    # df = pd.read_csv("../adult.csv")
    # df = df[["age", "education", "education.num", "race", "income"]]
    # df.rename(columns={'education.num': 'education_num'}, inplace=True)
    # df = df.sample(1000, random_state=0, ignore_index=True)
    # df = pd.read_csv("adult_100_sample.csv")
    # df.rename(columns={'education.num': 'education_num'}, inplace=True)
    # df["row_num"] = df.reset_index().index
    # print(df)

    # query_string = "SELECT COUNT(*) FROM adult WHERE age < 40 AND income == '>50K'"
    # query_string = "SELECT race, COUNT(*) FROM adult WHERE education_num >= 14 AND income == '<=50K' GROUP BY race"

    # query_string = "SELECT COUNT(*) FROM adult WHERE income == '>50K' AND education_num == 13 AND age == 25"
    query_string = "SELECT marital_status, COUNT(*) FROM adult WHERE race == 'Asian-Pac-Islander' AND age >= 30 AND " \
                   "age <= 40 GROUP BY marital_status"
    # query_string = "SELECT COUNT(*) FROM adult WHERE native_country != 'United-States' AND sex == 'Female'"
    # query_string = "SELECT AVG(hours_per_week) FROM adult WHERE workclass == 'Federal-gov' OR workclass ==
    # 'Local-gov' or workclass == 'State-gov'"
    # query_string = "SELECT SUM(fnlwgt) FROM adult WHERE capital_gain > 0 AND income == '<=50K' AND occupation ==
    # 'Sales'"

    # query_string = "SELECT sex, AVG(age) FROM adult GROUP BY sex"

    # query_string = "SELECT AVG(age) FROM adult"

    # query_string = "SELECT AVG(capital_loss) FROM adult WHERE hours_per_week > 40 AND workclass == 'Federal-gov' OR
    # workclass == 'Local-gov' or workclass == 'State-gov'"

    # design epsilons to test in a way that smaller eps are more frequent and largest eps are less
    # eps_list = list(np.arange(0.001, 0.01, 0.001, dtype=float))
    eps_list = list(np.arange(0.01, 0.1, 0.01, dtype=float))
    eps_list += list(np.arange(0.1, 1, 0.1, dtype=float))
    eps_list += list(np.arange(1, 11, 1, dtype=float))
    # eps_list = [0.01]

    start_time = time.time()
    eps = find_epsilon(df, query_string, eps_list, num_parallel_processes=2)
    elapsed = time.time() - start_time
    print(f"total time: {elapsed} s")

    print(eps)

    # times = []
    #
    # for i in range(1, 9):
    #
    #     start_time = time.time()
    #     eps = find_epsilon(df, query_string, -1, 100, eps_list, 1, 0.05, test="mw", num_parallel_processes=i)
    #     elapsed = time.time() - start_time
    #     print(f"total time: {elapsed} s")
    #
    #     print(eps)
    #
    #     times.append(elapsed)
    #
    # plt.plot(np.arange(1, 9), times)
    # plt.savefig("num_processes")
