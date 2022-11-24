import itertools
import math
import random
import re

import numpy as np
import pandas as pd
from scipy import stats
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
import matplotlib.pyplot as plt
from statsmodels.stats.multitest import fdrcorrection
from sql_metadata import Parser


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


def execute_rewritten_ast(sqlite_connection, table_name, row_num_col, row_num_to_exclude,
                          private_reader, subquery, query, *ignore, accuracy: bool = False, pre_aggregated=None,
                          postprocess=True):
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

        if row_num_to_exclude != -1:
            pos = query_string.rfind(" WHERE ")  # last pos of WHERE
            if pos == -1:
                # query_string = f"{query_string} WHERE {row_num_col} != {row_num_to_exclude}"
                pos2 = query_string.rfind(f" FROM {table_name}")
                pos2 += len(f" FROM {table_name}")
                query_string = query_string[:pos2] + f" WHERE {row_num_col} != {row_num_to_exclude} " + query_string[pos2:]
            else:
                # print(pos)
                pos += len(" WHERE ")
                # print(query_string[:pos])
                query_string = query_string[:pos] + f"{row_num_col} != {row_num_to_exclude} AND " + query_string[pos:]

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

    # get analytics_server list of mechanisms in column order
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
        # it's analytics_server dataframe
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
    # if private_reader._options.censor_dims:
    #     if kc_pos is None:
    #         raise ValueError("Query needs analytics_server key count column to censor dimensions")
    #     else:
    #         thresh_mech = mechs[kc_pos]
    #         private_reader.tau = thresh_mech.threshold
    #         # print("xxx")
    #     if hasattr(out, "filter"):
    #         # it's an RDD
    #         tau = private_reader.tau
    #         out = out.filter(lambda row: row[kc_pos] > tau)
    #         # print("yyy")
    #     else:
    #         # print(kc_pos)
    #         # print(private_reader.tau)
    #         out = filter(lambda row: row[kc_pos] > private_reader.tau, out)
    #         # print("zzz")

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
    if clamp_counts:
        if hasattr(out, "rdd"):
            # it's analytics_server dataframe
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
            # it's analytics_server dataframe
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
            # it's analytics_server dataframe
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


def compute_original_risks(df: pd.DataFrame, query_string: str, metadata: dict, dfs_one_off: list):
    privacy = Privacy(epsilon=np.inf, delta=0)  # no privacy

    # query_string = "SELECT COUNT(*) from adult.adult"
    # query_string = "SELECT race, COUNT(*) FROM adult.adult WHERE education_num >= 13 GROUP BY race"

    query_check = re.search(pattern="SELECT\s.*\(.*\)\sFROM\s.*",
                            string=query_string,
                            flags=re.IGNORECASE)
    if query_check is not None:
        pos = query_string.rfind(")")  # last pos of )
        assert pos > -1
        # print(pos)

        window = " OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS res "
        query_string_window = query_string[:pos + 1] + window + query_string[pos + 2:]

        query_string = query_string[:pos + 1] + " AS res " + query_string[pos + 2:]
        print(query_string)
        print(query_string_window)
    else:
        print("Query in wrong format")
        return None

    private_reader = snsql.from_df(df, metadata=metadata, privacy=privacy)
    original_result = private_reader.reader.execute_df(query_string)
    print("original_result")
    print(original_result)

    has_group_by = re.search(pattern="GROUP BY", string=query_string, flags=re.IGNORECASE)
    if has_group_by is None:
        # query_string_window = "SELECT DISTINCT race, COUNT(*) OVER (win ROWS BETWEEN UNBOUNDED PRECEDING AND
        # UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS res FROM adult.adult WHERE education_num >= 13 WINDOW win AS (
        # PARTITION BY race)"
        # query_string_window = "SELECT DISTINCT race, COUNT(*) OVER (PARTITION BY race ROWS BETWEEN UNBOUNDED
        # PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS res FROM adult.adult WHERE education_num >= 13"
        # query_string_window = "SELECT DISTINCT race, COUNT(*) OVER win FROM adult.adult WINDOW win AS (ROWS BETWEEN
        # UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW)"
        # query_string_window = "SELECT DISTINCT race, COUNT(*) OVER (PARTITION BY race) AS res FROM adult.adult
        # WHERE education_num >= 13"
        # query_string_window = "SELECT race, COUNT(*) FROM adult.adult WHERE row_num != 0 GROUP BY race; SELECT
        # race, " \
        #                       "COUNT(*) FROM adult.adult WHERE row_num != 1 GROUP BY race"

        private_reader = snsql.from_df(df, metadata=metadata, privacy=privacy)
        window_result = private_reader.reader.execute_df(query_string_window)
        print("window_result")
        print(window_result)

        original_risks = abs(window_result["res"].to_numpy() - original_result["res"].to_numpy())
        print(original_risks)
    else:
        # Compute the original privacy risks
        original_risks = []
        # dfs_one_off = []  # cache
        for i in range(len(df)):
            # cur_df = df.copy()
            # cur_df = cur_df.drop([i])
            # dfs_one_off.append(cur_df)
            cur_df = dfs_one_off[i]

            private_reader = snsql.from_df(cur_df, metadata=metadata, privacy=privacy)
            cur_result = private_reader.reader.execute_df(query_string)  # execute query without DP
            # print(type(private_reader.reader).__name__)
            # change = abs(cur_result - original_result).to_numpy().sum()
            change = abs(cur_result.sum(numeric_only=True).sum() - original_result.sum(numeric_only=True).sum())
            # print(cur_result)
            # print(original_result)
            # print(change)
            original_risks.append(change)

    return original_risks, original_result


def extract_table_names(query):
    """ Extract table names from an SQL query. """
    # analytics_server good old fashioned regex. turns out this worked better than actually parsing the code
    tables_blocks = re.findall(r'(?:FROM|JOIN)\s+(\w+(?:\s*,\s*\w+)*)', query, re.IGNORECASE)
    tables = [tbl
              for block in tables_blocks
              for tbl in re.findall(r'\w+', block)]
    return set(tables)


def find_epsilon(df: pd.DataFrame,
                 query_string: str,
                 risk_group_percentile: int,
                 risk_group_size: int,
                 eps_to_test: list,
                 num_runs: int,
                 q: float,
                 test: str = "mw"):
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

        start_time = time.time()

        # dfs_one_off = []  # cache
        # for i in range(len(df)):
        #     cur_df = df.copy()
        #     cur_df = cur_df.drop([i])
        #     dfs_one_off.append(cur_df)

        # TODO: cache risk scores by first finding the relevant columns in query, and save the risk score of each set of column values
        sql_parser = Parser(query_string)
        query_columns = sql_parser.columns
        # print(query_columns)
        df_copy = df.copy()
        df_copy = df_copy[query_columns]

        risk_score_cache = {}

        metadata = get_metadata(df_copy, table_name)

        # original_risks, original_result = compute_original_risks(df, query_string, metadata, dfs_one_off)

        engine = create_engine("sqlite:///:memory:")

        sqlite_connection = engine.connect()

        # just needed to create analytics_server row_num col that doesn't already exist
        row_num_col = f"row_num_{random.randint(0, 10000)}"
        while row_num_col in df_copy.columns:
            row_num_col = f"row_num_{random.randint(0, 10000)}"
        df_copy[row_num_col] = df_copy.reset_index().index

        num_rows = to_sql(df_copy, name=table_name, con=sqlite_connection,
                             index=not any(name is None for name in df_copy.index.names),
                             if_exists="replace")  # load index into db if all levels are named
        if num_rows != len(df_copy):
            print("error when loading to sqlite")
            return None

        # table_names = engine.table_names(connection=sqlite_connection)
        # print(table_names)

        original_result = read_sql(sql=query_string, con=sqlite_connection)
        # res = [tuple([col for col in q_result.columns])] + [val[1:] for val in q_result.itertuples()]
        print("original_result")
        print(original_result)
        # print(res)

        if len(original_result.select_dtypes(include=np.number).columns) > 1:
            print("error: can only have one numerical column in the query result")
            return None

        # print(read_sql(sql=f"SELECT * FROM {table_name}", con=sqlite_connection))

        original_risks = []

        query_string = re.sub(f" WHERE ", f" WHERE ", query_string, flags=re.IGNORECASE)
        query_string = re.sub(f" FROM ", f" FROM ", query_string, flags=re.IGNORECASE)

        pos = query_string.rfind(" WHERE ")  # last pos of WHERE
        no_where_clause = False
        if pos == -1:
            no_where_clause = True
            pos2 = query_string.rfind(f" FROM {table_name}")
            # print(query_string)
            # print(pos2)
        else:
            pos += len(" WHERE ")

        for i in range(len(df_copy)):

            column_values = tuple(df_copy.iloc[i][query_columns].values)
            # print(column_values)
            if column_values not in risk_score_cache.keys():

                if no_where_clause:
                    pos2 += len(f" FROM {table_name}")
                    cur_query = query_string[:pos2] + f" WHERE {row_num_col} != {i} " + query_string[pos2:]
                else:
                    cur_query = query_string[:pos] + f"{row_num_col} != {i} AND " + query_string[pos:]

                # print(cur_query)
                cur_result = read_sql(sql=cur_query, con=sqlite_connection)
                # print(cur_result)

                risk_score = abs(cur_result.sum(numeric_only=True).sum() - original_result.sum(numeric_only=True).sum())

                risk_score_cache[column_values] = risk_score
            else:
                risk_score = risk_score_cache[column_values]

            original_risks.append(risk_score)

        # print(original_risks)
        elapsed = time.time() - start_time
        print(f"time to compute original risk: {elapsed} s")

        # return None

        # Select groups we want to equalize risks
        # for now use the default strategy - high risk (upper x% quantile) and low risk (lower x% quantile) group
        sorted_original_risks = list(np.sort(original_risks))
        # print(sorted_original_risks)
        sorted_original_risks_idx = np.argsort(original_risks)
        # idx1 = sorted_original_risks.index(
        #     np.percentile(sorted_original_risks, risk_group_percentile, interpolation='nearest'))

        if risk_group_percentile > 0:

            if risk_group_percentile > 50:
                risk_group_percentile = 100 - risk_group_percentile

            val1 = np.percentile(sorted_original_risks, risk_group_percentile, interpolation='nearest')
            # last index of val1
            idx1 = max(loc for loc, val in enumerate(sorted_original_risks) if abs(val - val1) <= 10e-8)

            # idx2 = sorted_original_risks.index(
            #     np.percentile(sorted_original_risks, 100 - risk_group_percentile, interpolation='nearest'))
            val2 = np.percentile(sorted_original_risks, 100 - risk_group_percentile, interpolation='nearest')
            # first index of val2
            idx2 = sorted_original_risks.index(val2)

        else:

            if risk_group_size > len(sorted_original_risks):
                print("risk_group_size larger than total size")
                return None

            idx1 = risk_group_size - 1
            idx2 = len(sorted_original_risks) - risk_group_size

        # print(val1, val2)
        # if idx1 > idx2:
        #     idx1, idx2 = idx2, idx1
        print("risk group idx:", idx1, idx2)

        # sample1 = sorted_original_risks[:idx1 + 1]
        # sample2 = sorted_original_risks[idx2:]
        # print(sample1)
        # print(sample2)
        sample_idx1 = sorted_original_risks_idx[:idx1 + 1]
        sample_idx2 = sorted_original_risks_idx[idx2:]
        # print(sample_idx1, sample_idx2)

        # Check whether the samples come from the same distribution (null hypothesis)
        # Reject the null if p-value is less than some threshold
        # for now we don't care even if the samples already come from the same distribution
        # res = stats.ks_2samp(sample1, sample2)
        # p_value = res[1]
        # print(f"p_value: {p_value}")

        # Now we test different epsilons
        # For each epsilon, we run n times, with the goal of finding the largest epsilon
        # that can constantly stay in null for all n runs

        best_eps = None

        # We start from the largest epsilon to the smallest
        sorted_eps_to_test = np.sort(eps_to_test)[::-1]
        # print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        # print(sorted_eps_to_test)
        # return None

        compute_risk_time = 0.0
        test_equal_distrbution_time = 0.0

        # query_string = query_string.replace(f" {table_name} ", f" {table_name}.{table_name} ")
        query_string = re.sub(f" FROM {table_name}", f" FROM {table_name}.{table_name}", query_string, flags=re.IGNORECASE)

        for eps in tqdm.tqdm(sorted_eps_to_test):

            print(f"epsilon = {eps}")

            # reject_null = False
            p_values = []

            # For each run, we compute the risks again
            # New risk = DP result - DP result when removing one record

            privacy = Privacy(epsilon=eps)
            private_reader = snsql.from_df(df_copy, metadata=metadata, privacy=privacy)
            # dp_result = private_reader.execute_df(query)
            # rewrite the query ast once for every epsilon
            query_ast = private_reader.parse_query_string(query_string)
            # print(query_ast)
            subquery, query = private_reader._rewrite_ast(query_ast)
            # print(subquery)
            # print(query)

            for j in tqdm.tqdm(range(num_runs)):

                # if reject_null:
                #     continue # this eps does not equalize risks, skip

                start_time = time.time()

                # better to compute analytics_server new DP result each run
                # dp_result = private_reader.execute_df(query)
                # query_ast = private_reader.parse_query_string(query_string)
                # subquery, query = private_reader._rewrite_ast(query_ast)
                dp_result = execute_rewritten_ast(sqlite_connection, table_name, row_num_col, -1, private_reader, subquery, query)
                dp_result = private_reader._to_df(dp_result)
                # print(dp_result)

                # We get the same samples based on individuals who are present in the original chosen samples
                # we only need to compute risks for those samples
                new_risks1 = []
                new_risks2 = []

                risk_score_cache_dp = {}

                for i in sample_idx1:
                    # cur_df = dfs_one_off[i]

                    # private_reader = snsql.from_df(cur_df, metadata=metadata, privacy=privacy)
                    # cur_result = private_reader.execute_df(query)

                    column_values = tuple(df_copy.iloc[i][query_columns].values)
                    # print(column_values)
                    if column_values not in risk_score_cache_dp.keys():

                        cur_result = execute_rewritten_ast(sqlite_connection, table_name, row_num_col, i, private_reader, subquery, query)
                        cur_result = private_reader._to_df(cur_result)

                        # change = abs(cur_result - dp_result).to_numpy().sum()
                        risk_score = 0
                        if len(cur_result) > 0 and len(dp_result) > 0:
                            risk_score = abs(cur_result.sum(numeric_only=True).sum() - dp_result.sum(numeric_only=True).sum())

                        risk_score_cache_dp[column_values] = risk_score

                    else:
                        risk_score = risk_score_cache_dp[column_values]

                    new_risks1.append(risk_score)

                for i in sample_idx2:
                    # cur_df = dfs_one_off[i]

                    column_values = tuple(df_copy.iloc[i][query_columns].values)

                    if column_values not in risk_score_cache_dp.keys():

                        # private_reader = snsql.from_df(cur_df, metadata=metadata, privacy=privacy)
                        cur_result = execute_rewritten_ast(sqlite_connection, table_name, row_num_col, i, private_reader, subquery, query)
                        cur_result = private_reader._to_df(cur_result)

                        # change = abs(cur_result - dp_result).to_numpy().sum()
                        risk_score = 0
                        if len(cur_result) > 0 and len(dp_result) > 0:
                            risk_score = abs(cur_result.sum(numeric_only=True).sum() - dp_result.sum(numeric_only=True).sum())

                        risk_score_cache_dp[column_values] = risk_score

                    else:
                        risk_score = risk_score_cache_dp[column_values]

                    new_risks2.append(risk_score)


                # normalize
                s1 = sum(new_risks1)
                if s1 != 0:
                    new_risks1 = [float(x) / s1 for x in new_risks1]
                s2 = sum(new_risks2)
                if s2 != 0:
                    new_risks2 = [float(x) / s2 for x in new_risks2]

                elapsed = time.time() - start_time
                # print(f"{j}th compute risk time: {elapsed} s")
                compute_risk_time += elapsed

                # We perform the test and record the p-value for each run
                start_time = time.time()

                if test == "mw":
                    cur_res = stats.mannwhitneyu(new_risks1, new_risks2)
                elif test == "ks":
                    cur_res = stats.ks_2samp(new_risks1, new_risks2)  # , method="exact")
                elif test == "es":
                    cur_res = stats.epps_singleton_2samp(new_risks1, new_risks2)

                p_value = cur_res[1]
                # if p_value < 0.01:
                #     reject_null = True # early stopping
                p_values.append(p_value)

                # print(pd.DataFrame(new_risks1).describe())
                # print(pd.DataFrame(new_risks2).describe())
                # print(f"p_value: {p_value}")

                elapsed = time.time() - start_time
                test_equal_distrbution_time += elapsed

            # Now we have n p-values for the current epsilon, we use multiple comparisons' technique
            # to determine if this epsilon is good enough
            # For now we use false discovery rate
            # print(p_values)
            if not fdr(p_values, q):  # q = proportion of false positives we will accept
                # We want no discovery (fail to reject null) for all n runs
                # If we fail to reject the null, then we break the loop.
                # The current epsilon is the one we choose
                best_eps = eps
                break

            # TODO: if we find a lot of discoveries (a lot of small p-values),
            #  we can skip epsilons that are close to the current eps (ex. 10 to 9).
            #  If there's only a few discoveries,
            #  we should probe epsilons that are close (10 to 9.9)

        print(f"total time to compute new risk: {compute_risk_time} s")
        print(f"total time to test equal distribution: {test_equal_distrbution_time} s")

        sqlite_connection.close()

        if best_eps is None:
            return None

        return best_eps, dp_result # also return the dp result computed


def find_epsilon_us(df: pd.DataFrame,
                 query_string: str,
                 risk_group_percentile: int,
                 eps_to_test: list,
                 num_runs: int,
                 q: float):
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

        start_time = time.time()

        # dfs_one_off = []  # cache
        # for i in range(len(df)):
        #     cur_df = df.copy()
        #     cur_df = cur_df.drop([i])
        #     dfs_one_off.append(cur_df)

        metadata = get_metadata(df, table_name)
        # print(metadata)

        # original_risks, original_result = compute_original_risks(df, query_string, metadata, dfs_one_off)

        engine = create_engine("sqlite:///:memory:")

        sqlite_connection = engine.connect()

        df_copy = df.copy()

        # just needed to create analytics_server row_num col that doesn't already exist
        row_num_col = f"row_num_{random.randint(0, 10000)}"
        while row_num_col in df_copy.columns:
            row_num_col = f"row_num_{random.randint(0, 10000)}"
        df_copy[row_num_col] = df_copy.reset_index().index

        num_rows = to_sql(df_copy, name=table_name, con=sqlite_connection,
                             index=not any(name is None for name in df_copy.index.names),
                             if_exists="replace")  # load index into db if all levels are named
        if num_rows != len(df_copy):
            print("error when loading to sqlite")
            return None

        # table_names = engine.table_names(connection=sqlite_connection)
        # print(table_names)

        original_result = read_sql(sql=query_string, con=sqlite_connection)
        # res = [tuple([col for col in q_result.columns])] + [val[1:] for val in q_result.itertuples()]
        print("original_result")
        print(original_result)
        # print(res)

        if len(original_result.select_dtypes(include=np.number).columns) > 1:
            print("error: can only have one numerical column in the query result")
            return None

        # print(read_sql(sql=f"SELECT * FROM {table_name}", con=sqlite_connection))

        original_risks = []

        query_string = re.sub(f" WHERE ", f" WHERE ", query_string, flags=re.IGNORECASE)
        query_string = re.sub(f" FROM ", f" FROM ", query_string, flags=re.IGNORECASE)

        pos = query_string.rfind(" WHERE ")  # last pos of WHERE
        no_where_clause = False
        if pos == -1:
            no_where_clause = True
            pos2 = query_string.rfind(f" FROM {table_name}") + len(f" FROM {table_name}")
            # print(query_string)
            # print(pos2)
        else:
            pos += len(" WHERE ")

        for i in range(len(df_copy)):

            if no_where_clause:
                cur_query = query_string[:pos2] + f" WHERE {row_num_col} != {i} " + query_string[pos2:]
            else:
                cur_query = query_string[:pos] + f"{row_num_col} != {i} AND " + query_string[pos:]

            # if cur_query.count("SELECT ") > 1:
            #     # there's subquery
            #     for m in re.finditer("SELECT ", cur_query):
            #         pos = m.end()
            #         cur_query = query_string[:pos] + f"{row_num_col}, " + query_string[pos:]

            # print(cur_query)
            cur_result = read_sql(sql=cur_query, con=sqlite_connection)
            # print(cur_result)

            change = abs(cur_result.sum(numeric_only=True).sum() - original_result.sum(numeric_only=True).sum())
            original_risks.append(change)

        # print(original_risks)
        elapsed = time.time() - start_time
        print(f"time to compute original risk: {elapsed} s")

        # return None

        # Select groups we want to equalize risks
        # for now use the default strategy - high risk (upper x% quantile) and low risk (lower x% quantile) group
        sorted_original_risks = list(np.sort(original_risks))
        # print(sorted_original_risks)
        sorted_original_risks_idx = np.argsort(original_risks)
        # idx1 = sorted_original_risks.index(
        #     np.percentile(sorted_original_risks, risk_group_percentile, interpolation='nearest'))

        if risk_group_percentile > 50:
            risk_group_percentile = 100 - risk_group_percentile

        val1 = np.percentile(sorted_original_risks, risk_group_percentile, interpolation='nearest')
        # last index of val1
        idx1 = max(loc for loc, val in enumerate(sorted_original_risks) if abs(val - val1) <= 10e-8)

        # idx2 = sorted_original_risks.index(
        #     np.percentile(sorted_original_risks, 100 - risk_group_percentile, interpolation='nearest'))
        val2 = np.percentile(sorted_original_risks, 100 - risk_group_percentile, interpolation='nearest')
        # first index of val2
        idx2 = sorted_original_risks.index(val2)

        # print(sorted_original_risks)
        # print(val1, val2)
        # if idx1 > idx2:
        #     idx1, idx2 = idx2, idx1
        # idx1 = 2
        # idx2 = 97
        # print(idx1, idx2)

        # sample1 = sorted_original_risks[:idx1 + 1]
        # sample2 = sorted_original_risks[idx2:]
        # print(sample1)
        # print(sample2)
        sample_idx1 = sorted_original_risks_idx[:idx1 + 1]
        sample_idx2 = sorted_original_risks_idx[idx2:]
        # print(sample_idx1, sample_idx2)
        # top_3_sample_idx1 = sorted_original_risks_idx[:3]
        # bottom_3_sample_idx2 = sorted_original_risks_idx[len(df)-3:]


        # Check whether the samples come from the same distribution (null hypothesis)
        # Reject the null if p-value is less than some threshold
        # for now we don't care even if the samples already come from the same distribution
        # res = stats.ks_2samp(sample1, sample2)
        # p_value = res[1]
        # print(f"p_value: {p_value}")

        # Now we test different epsilons
        # For each epsilon, we run n times, with the goal of finding the largest epsilon
        # that can constantly stay in null for all n runs

        best_eps = None

        # We start from the largest epsilon to the smallest
        sorted_eps_to_test = np.sort(eps_to_test)[::-1]
        # print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        # print(sorted_eps_to_test)
        # return None

        compute_risk_time = 0.0
        test_equal_distrbution_time = 0.0

        # query_string = query_string.replace(f" {table_name} ", f" {table_name}.{table_name} ")
        query_string = re.sub(f" FROM {table_name}", f" FROM {table_name}.{table_name}", query_string, flags=re.IGNORECASE)

        risks = []
        max_risk = 0

        for eps in tqdm.tqdm(sorted_eps_to_test):

            print(f"epsilon = {eps}")

            # reject_null = False
            p_values = []

            # For each run, we compute the risks again
            # New risk = DP result - DP result when removing one record

            privacy = Privacy(epsilon=eps)
            private_reader = snsql.from_df(df_copy, metadata=metadata, privacy=privacy)
            # private_reader._options.censor_dims = False
            # private_reader.rewriter.options.censor_dims = False
            # dp_result = private_reader.execute_df(query)
            # rewrite the query ast once for every epsilon
            # TODO: might be able to do it once if epsilon is not involved in rewriting
            query_ast = private_reader.parse_query_string(query_string)
            # print(query_ast)
            subquery, query = private_reader._rewrite_ast(query_ast)
            # print(subquery)
            # print(query)

            from collections import defaultdict
            # top_3_risk = defaultdict(list)
            # bottom_3_risk = defaultdict(list)
            new_risks = defaultdict(list)

            for j in tqdm.tqdm(range(num_runs)):

                # if reject_null:
                #     continue # this eps does not equalize risks, skip

                start_time = time.time()

                # better to compute analytics_server new DP result each run
                # dp_result = private_reader.execute_df(query)
                # query_ast = private_reader.parse_query_string(query_string)
                # subquery, query = private_reader._rewrite_ast(query_ast)
                dp_result = execute_rewritten_ast(sqlite_connection, table_name, row_num_col, -1, private_reader, subquery, query)
                # print(dp_result)
                dp_result = private_reader._to_df(dp_result)
                # print("dp_result", dp_result)

                for i in sorted_original_risks_idx:
                    # cur_df = dfs_one_off[i]

                    # private_reader = snsql.from_df(df_copy, metadata=metadata, privacy=privacy)
                    # cur_result = private_reader.execute_df(query)

                    cur_result = execute_rewritten_ast(sqlite_connection, table_name, row_num_col, i, private_reader,
                                                       subquery, query)
                    cur_result = private_reader._to_df(cur_result)
                    # print("cur_result", cur_result)

                    # change = abs(cur_result - dp_result).to_numpy().sum()
                    change = 0
                    if len(cur_result) > 0 and len(dp_result) > 0:
                        change = abs(cur_result.sum(numeric_only=True).sum() - dp_result.sum(numeric_only=True).sum())
                    # print(change)
                    new_risks[i].append(change)

                '''
                # We get the same samples based on individuals who are present in the original chosen samples
                # we only need to compute risks for those samples
                new_risks1 = []
                new_risks2 = []

                for i in sample_idx1:
                    # cur_df = dfs_one_off[i]

                    # private_reader = snsql.from_df(cur_df, metadata=metadata, privacy=privacy)
                    # cur_result = private_reader.execute_df(query)

                    cur_result = execute_rewritten_ast(sqlite_connection, table_name, row_num_col, i, private_reader, subquery, query)
                    cur_result = private_reader._to_df(cur_result)

                    # change = abs(cur_result - dp_result).to_numpy().sum()
                    change = 0
                    if len(cur_result) > 0 and len(dp_result) > 0:
                        change = abs(cur_result.sum(numeric_only=True).sum() - dp_result.sum(numeric_only=True).sum())

                    new_risks1.append(change)

                    # if i in top_3_sample_idx1:
                    #     top_3_risk[i].append(change)

                for i in sample_idx2:
                    # cur_df = dfs_one_off[i]

                    # private_reader = snsql.from_df(cur_df, metadata=metadata, privacy=privacy)
                    cur_result = execute_rewritten_ast(sqlite_connection, table_name, row_num_col, i, private_reader, subquery, query)
                    cur_result = private_reader._to_df(cur_result)

                    # change = abs(cur_result - dp_result).to_numpy().sum()
                    change = 0
                    if len(cur_result) > 0 and len(dp_result) > 0:
                        change = abs(cur_result.sum(numeric_only=True).sum() - dp_result.sum(numeric_only=True).sum())

                    new_risks2.append(change)

                    # if i in bottom_3_sample_idx2:
                    #     bottom_3_risk[i].append(change)

                # normalize
                # sum1 = sum(new_risks1)
                # if sum1 != 0:
                #     new_risks1 = [float(x) / sum1 for x in new_risks1]
                #     for k, lst in top_3_risk.items():
                #         top_3_risk[k][-1] = top_3_risk[k][-1] / sum1
                # sum2 = sum(new_risks2)
                # if sum2 != 0:
                #     new_risks2 = [float(x) / sum2 for x in new_risks2]
                #     for k, lst in bottom_3_risk.items():
                #         bottom_3_risk[k][-1] = bottom_3_risk[k][-1] / sum2

                elapsed = time.time() - start_time
                # print(f"{j}th compute risk time: {elapsed} s")
                compute_risk_time += elapsed

                # We perform the test and record the p-value for each run
                start_time = time.time()

                cur_res = stats.ks_2samp(new_risks1, new_risks2)  # , method="exact")
                p_value = cur_res[1]
                # if p_value < 0.01:
                #     reject_null = True # early stopping
                p_values.append(p_value)

                # print(pd.DataFrame(new_risks1).describe())
                # print(pd.DataFrame(new_risks2).describe())
                # print(f"p_value: {p_value}")

                elapsed = time.time() - start_time
                test_equal_distrbution_time += elapsed

            # Now we have n p-values for the current epsilon, we use multiple comparisons' technique
            # to determine if this epsilon is good enough
            # For now we use false discovery rate
            # print(p_values)
            if not fdr(p_values, q):  # q = proportion of false positives we will accept
                # We want no discovery (fail to reject null) for all n runs
                # If we fail to reject the null, then we break the loop.
                # The current epsilon is the one we choose
                best_eps = eps
                print(f"best eps = {eps}")
                # break

            # break
            '''

            # avg_risks = [np.average(lst) for lst in new_risks.values()]
            med_risks = [np.median(lst) for lst in new_risks.values()]
            # normalize
            s = sum(med_risks)
            if s != 0:
                med_risks = [float(x) / s * 100 for x in med_risks]
            print(pd.DataFrame(med_risks).describe())
            risks.append(med_risks)

            if max_risk < max(med_risks):
                max_risk = max(med_risks)

            # plt.rcParams['figure.figsize'] = [3, 2]
            # # plt.rcParams['figure.dpi'] = 100
            # plt.hist(med_risks)
            # # plt.xlim(xmin=0)
            # # plt.xlabel("risk score")
            # # plt.ylabel("count")
            # plt.tight_layout()
            # plt.savefig(f"figures/q1_eps_{eps}.jpg")
            # plt.close()

            # if eps == 1:
            #     break

            # TODO: if we find analytics_server lot of discoveries (analytics_server lot of small p-values),
            #  we can skip epsilons that are close to the current eps (ex. 10 to 9).
            #  If there's only analytics_server few discoveries,
            #  we should probe epsilons that are close (10 to 9.9)

            # print("top 3", [np.average(lst) for lst in top_3_risk.values()])
            # print("bottom 3", [np.average(lst) for lst in bottom_3_risk.values()])

        print(f"total time to compute new risk: {compute_risk_time} s")
        print(f"total time to test equal distribution: {test_equal_distrbution_time} s")

        sqlite_connection.close()

        # return

        s = sum(original_risks)
        if s != 0:
            original_risks = [float(x) / s * 100 for x in original_risks]
        print(pd.DataFrame(original_risks).describe())

        if max_risk < max(original_risks):
            max_risk = max(original_risks)

        plt.rcParams['figure.figsize'] = [3, 2]

        medianprops = dict(linestyle=None, linewidth=0)
        whiskerprops = dict(linewidth=2)
        capprops = dict(linewidth=2)
        boxprops = dict(linestyle=None, linewidth=0)

        # plt.yticks(fontsize=14)

        for i, med_risks in enumerate(risks):

            # plt.rcParams['figure.dpi'] = 100
            # plt.hist(med_risks)


            # plt.boxplot(med_risks,
            #             whis=[0, 100],
            #             medianprops=medianprops,
            #             whiskerprops=whiskerprops,
            #             capprops=capprops,
            #             boxprops=boxprops)
            plt.errorbar(x=1, y=0, yerr=[[min(med_risks)], [max(med_risks)]], fmt='none', capsize=10, elinewidth=2, capthick=2)
            # plt.xlim(xmin=-1, xmax=math.ceil(max_risk)) #math.ceil(max_risk)
            # plt.ylim(ymax=13)
            plt.yticks(ticks=[0, 2.5, 5.0, 7.5, 10.0, 12.5], labels=[0, 2.5, 5.0, 7.5, 10.0, 12.5])
            # plt.xlim(xmin=0)
            # plt.xlabel("risk score")
            plt.ylabel("Range of Risk Scores")
            # plt.tick_params(
            #     axis='x',  # changes apply to the x-axis
            #     which='both',  # both major and minor ticks are affected
            #     bottom=False,  # ticks along the bottom edge are off
            #     top=False,  # ticks along the top edge are off
            #     labelbottom=False)  # labels along the bottom edge are off
            plt.xticks([], [])
            plt.tight_layout()
            plt.savefig(f"figures/q1_eps_{sorted_eps_to_test[i]:0.1f}_box.jpg")
            plt.close()

        # plt.hist(original_risks)
        # plt.boxplot(original_risks,
        #             whis=[0, 100],
        #             medianprops=medianprops,
        #             whiskerprops=whiskerprops,
        #             capprops=capprops,
        #             boxprops=boxprops)
        plt.errorbar(x=1, y=0, yerr=[[min(original_risks)], [max(original_risks)]], fmt='none', capsize=10, elinewidth=2, capthick=2)
        # plt.xlim(xmin=0, xmax=math.ceil(max(original_risks)))
        # plt.ylim(ymax=13)
        plt.yticks(ticks=[0, 2.5, 5.0, 7.5, 10.0, 12.5], labels=[0, 2.5, 5.0, 7.5, 10.0, 12.5])

        plt.ylabel("Range of Risk Scores")
        plt.xticks([], [])
        # plt.xlabel("risk score")
        # plt.ylabel("count")
        plt.tight_layout()

        # plt.tick_params(
        #     axis='x',  # changes apply to the x-axis
        #     which='both',  # both major and minor ticks are affected
        #     bottom=False,  # ticks along the bottom edge are off
        #     top=False,  # ticks along the top edge are off
        #     labelbottom=False)  # labels along the bottom edge are off

        plt.savefig(f"figures/q1_original_box.jpg")
        plt.close()

        return best_eps


if __name__ == '__main__':
    # csv_path = '../PUMS.csv'
    # df = pd.read_csv(csv_path)#.head(100)
    # print(df.head())
    df = pd.read_csv("../adult.csv")
    # df = df[["age", "education", "education.num", "race", "income"]]
    # df.rename(columns={'education.num': 'education_num'}, inplace=True)
    # df = df.sample(1000, random_state=0, ignore_index=True)
    # df = pd.read_csv("adult_100_sample.csv")
    df.rename(columns={'education.num': 'education_num'}, inplace=True)
    # df["row_num"] = df.reset_index().index
    # print(df)

    query_string = "SELECT COUNT(*) FROM adult WHERE age < 40 AND income == '>50K'"
    # query_string = "SELECT AVG(age) FROM adult WHERE income == '>50K' AND education_num == 13" #education_num == 13
    # query_string = "SELECT race, COUNT(*) FROM adult WHERE education_num >= 14 AND income == '<=50K' GROUP BY race"
    # query_string = "SELECT AVG(age) from PUMS"
    # query_string = "SELECT COUNT(*) FROM (SELECT COUNT(age) as cnt FROM adult GROUP BY age) WHERE cnt > 2"

    # design epsilons to test in analytics_server way that smaller eps are more frequent and largest eps are less
    eps_list = list(np.arange(0.001, 0.01, 0.001, dtype=float))
    eps_list += list(np.arange(0.01, 0.11, 0.01, dtype=float))
    eps_list += list(np.arange(0.1, 1.1, 0.1, dtype=float))
    eps_list += list(np.arange(1, 11, 1, dtype=float))
    # eps_list += list(np.arange(10, 101, 10, dtype=float))
    # eps_list = list(np.arange(0.5, 5.1, 0.5, dtype=float))

    # eps_list = [6.0]
    # print(eps_list)

    start_time = time.time()
    eps = find_epsilon(df, query_string, -1, 100, eps_list, 5, 0.05)
    elapsed = time.time() - start_time
    print(f"total time: {elapsed} s")

    print(eps)
