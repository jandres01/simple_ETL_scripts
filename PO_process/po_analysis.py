"""
Automate PO Gen Process
"""

import numpy as np
import pandas as pd
from datetime import date, timedelta, datetime

def sales_columns():
    """
    Function for listing sales columns
    """
    lst = ["CardCode", "ItemCode", "AveMonthlySalesQty"]
    return lst

def inv_columns():
    """
    Function to list columns needed from inventory
    """
    lst = ["WhsCode", "itemcode", "QtyOnHand"]
    return lst

def mdq_columns():
    """
    Function for selecting relevant Branches columns
    """
    lst = ["PLU", "UoM", "MDQ", "SRP", "TotalSRP"]
    return lst

def branch_columns():
    """
    Function for selecting relevant Branch columns
    """
    lst = ["CardCode", "CardName", "StoreClassification", "Type",
           "OrderingLimit",	"Leadtime"]

    return lst

def order_limit_columns():
    """
    Function for selecting relevant Branches columns
    """
    lst = ["Type", "OrderingLimit"]
    return lst

def item_master_columns():
    """
    Function for selecting relevant Branches columns
    """
    lst = ["PLU", "Description", "UoMGroup", "BaseUoM", "ItemGroupName"]
    return lst

def extract_first_value(lst_cols, df_raw):
    """
    Function to extract first elem in list within a columns
    """
    for elem in lst_cols:
        df_raw[elem] = (df_raw[elem]
                         .apply(lambda row: row[0] if
                                isinstance(row, np.ndarray) else row))
    return df_raw

def read_raw_files(path="../../data/raw/PO/P.O Gen Sample Raw Data From SAP_Rev.xlsx"):
    """
    Function to read raw files
    """
    sheets = ["Sales", "BranchesInventory", "MDQ", "Branches", "Ordering Limit", "ItemMaster"]

    sales_cols = sales_columns()
    df_sales = pd.read_excel(path, sheet_name=sheets[0])[sales_cols]
    df_sales = (df_sales.groupby(sales_cols[:-1]).agg({sales_cols[-1]:"sum"}).reset_index()
                .rename(columns={'CardCode':'BranchCode'}))
    print("Sales DF: %i" % len(df_sales))

    inv_cols = inv_columns()
    df_inv = (pd.read_excel(path, sheet_name=sheets[1])[inv_cols]
              .rename(columns={'WhsCode':'BranchCode', 'itemcode':'ItemCode'}))
    df_inv = df_inv.groupby(["BranchCode", "ItemCode"])[inv_cols[-1]].sum()

    mdq_cols = mdq_columns()
    df_mdq = (pd.read_excel(path, sheet_name=sheets[2])[mdq_cols]
              .rename(columns={'PLU':'ItemCode', "SRP":"UnitCost",
                               "UoM": "UOM"}))
    dict_mdq = {"UOM":"unique", "MDQ":'max', "TotalSRP":'sum', "UnitCost":"max"}
    df_mdq = df_mdq.groupby(["ItemCode"]).agg(dict_mdq)
    df_mdq['UOM'] = (df_mdq['UOM']
                     .apply(lambda row: row[0] if isinstance(row, np.ndarray) else row))

    branch_cols = branch_columns()
    df_branches = (pd.read_excel(path, sheet_name=sheets[3])[branch_cols]
                   .rename(columns={'CardCode':'BranchCode'}))
    dict_branch = {"StoreClassification":"unique", "Type":"unique", "CardName":"unique",
                   'Leadtime':'max', "OrderingLimit":'max'}
    df_branches = df_branches.groupby(["BranchCode"]).agg(dict_branch)
    df_branches['Type'] = (df_branches['Type']
                      .apply(lambda row: row[0] if isinstance(row, np.ndarray) else row))
    df_branches['StoreClassification'] = (df_branches['StoreClassification']
                     .apply(lambda row: row[0] if isinstance(row, np.ndarray) else row))
    df_branches['CardName'] = (df_branches['CardName']
                     .apply(lambda row: row[0] if isinstance(row, np.ndarray) else row))

    limit_cols = order_limit_columns()
    df_limit = pd.read_excel(path, sheet_name=sheets[4])[limit_cols].set_index("Type")

    item_cols = item_master_columns()
    df_item = (pd.read_excel(path, sheet_name=sheets[5])[item_cols]
               .rename(columns={'PLU':'ItemCode', "Description":"ItemName"}))
    agg_item = {"ItemName":"unique", "UoMGroup":"unique", "BaseUoM":"unique",
                "ItemGroupName":"unique"}
    df_item = df_item.groupby(["ItemCode"]).agg(agg_item)
    df_item['ItemName'] = (df_item['ItemName']
                     .apply(lambda row: row[0] if isinstance(row, np.ndarray) else row))
    df_item['UoMGroup'] = (df_item['UoMGroup']
                     .apply(lambda row: row[0] if isinstance(row, np.ndarray) else row))
    df_item['BaseUoM'] = (df_item['BaseUoM']
                     .apply(lambda row: row[0] if isinstance(row, np.ndarray) else row))
    df_item['ItemGroupName'] = (df_item['ItemGroupName']
                     .apply(lambda row: row[0] if isinstance(row, np.ndarray) else row))

    return df_sales, df_inv, df_mdq, df_branches, df_limit, df_item

def merge_item_branch(df_item, df_branch):
    """
    Add all inventories to every branch
    """
    df_item = df_item.reset_index()
    list_item = df_item.values.tolist()
    item_cols = df_item.columns.tolist()
    df_branch = df_branch.reset_index()
    list_branch = df_branch.values.tolist()
    branch_cols = df_branch.columns.tolist()

    join_columns = item_cols + branch_cols

    new_list = [item + branch for item in list_item for branch in list_branch]

    df_item_branch = pd.DataFrame(new_list, columns=join_columns)
    return df_item_branch

def join_datasets(df_item_branch, df_sales, df_inv, df_mdq, df_limit):
    """
    Join Sales, inventory MDQ, Branches & Ordering Limit
    """

    index_item = ["ItemCode", "BranchCode"]
    df_sales_item = pd.merge(df_item_branch, df_sales, on=index_item, how='left')

    index_sales_inv = ['ItemCode','BranchCode']
    df_sales_inv = pd.merge(df_sales_item, df_inv, on=index_sales_inv, how='left')
    df_sales_inv = (df_sales_inv.rename(columns={'AveMonthlySalesQty':'sales_offtake',
                                                 'QtyOnHand':'month_end_inventory'}))
    df_sales_inv.to_csv("../../data/interim/df_sales_inv.csv")

    index_sales_mdq = ["ItemCode"]
    df_sales_mdq = pd.merge(df_sales_inv, df_mdq, on=index_sales_mdq, how='left')

    df_sales_mdq.to_csv("../../data/interim/df_sales_branch.csv")

    return df_sales_mdq

def add_transaction_data(df_raw):
    """
    Function to add transaction metadata into dataframe
    """
    print(df_raw.columns)
    today = date.today()
    days_later = (today + timedelta(days=3)).strftime("%m-%d-%Y")
    today = today.strftime("%m-%d-%Y")
    df_raw['POSType'] = "ORW"
    df_raw['TransType'] = "StockRequest"
    df_raw['DocNumber'] = "SR" + df_raw['BranchCode'] + today + "MAIN"
    df_raw['frmWhse'] = 'MAIN'
    df_raw['toWhse'] = df_raw['BranchCode']
    df_raw['DocDate'] = today
    df_raw['DocDueDate'] = days_later
    df_raw['Remarks'] = ''

    agg_header = {"POSType":"unique", "TransType":"unique", "DocNumber":"unique",
                  "frmWhse":"unique", "toWhse":"unique", "DocDate":"unique",
                  "DocDueDate":"unique", "Remarks":"unique"}
    df_header = df_raw.groupby(['BranchCode']).agg(agg_header)
    lst_header = ["POSType", "TransType", "DocNumber", "frmWhse", "toWhse",
                  "DocDate", "DocDueDate", "Remarks"]
    df_header = extract_first_value(lst_header, df_header).reset_index()

    df_raw['GrossPrice'] = 0
    df_raw['TaxAmt'] = 0
    df_raw['DescAmt'] = 0
    df_raw['NetSales'] = 0
    df_raw['GrossSales'] = 0

    line_cols = ['BranchCode', 'DocNumber', 'frmWhse', 'toWhse', 'ItemCode', 'ItemName',
                 'UOM', 'UnitCost', 'Qty', 'GrossPrice', 'TaxAmt', 'DescAmt', "NetSales",
                 'GrossSales']
    df_line = df_raw[line_cols]
    df_line = df_line.dropna(how = 'any', subset=['Qty'])
    return df_header, df_line

def analyze_results(df_raw):
    """
    Create additional fields
    """
    df_raw['Ave. Daily Sales offtake'] = (df_raw['sales_offtake'] / 30).round(2)
    df_raw[["month_end_inventory", "Ave. Daily Sales offtake",
             "sales_offtake", "MDQ", "TotalSRP"]] = df_raw[["month_end_inventory",
                         "Ave. Daily Sales offtake", "sales_offtake",
                         "MDQ", "TotalSRP"]].fillna(0)
    df_raw['days_to_sell'] = (df_raw['month_end_inventory'] /
                              df_raw['Ave. Daily Sales offtake']).apply(np.ceil).fillna(0)
    df_raw['No. of Days per Month'] = 30
    df_raw.loc[df_raw['month_end_inventory'] < df_raw['sales_offtake'],
                'suggested_po'] = ((df_raw['Ave. Daily Sales offtake'] *
                                    df_raw['Leadtime'])
                                   - df_raw['month_end_inventory']).round(0).fillna(0)

    df_raw.loc[df_raw['MDQ'] > 0,
                'conversion'] = (df_raw['suggested_po'] / df_raw['MDQ']).round(0)

    df_raw['Qty'] = df_raw['conversion']
    df_raw['total_amount_order'] = df_raw['TotalSRP'] * df_raw['Qty']
    print("DF row: %i" % len(df_raw))
    df_raw = df_raw[df_raw['Qty']!=0]
    print("DF row: %i" % len(df_raw))
    return df_raw

def main():
    """
    Main
    """
    df_sales, df_inv, df_mdq, df_branches, df_limit, df_item = read_raw_files()
    df_item_branch = merge_item_branch(df_item, df_branches)

    df_join = join_datasets(df_item_branch, df_sales, df_inv, df_mdq, df_limit)
    df_join = df_join.dropna(how = 'any', subset=['BranchCode', 'CardName'])

    print(len(df_join[df_join['BranchCode'] == "01-00002"]))
    print(len(df_join[df_join['BranchCode'] == "01-00118"]))

    df_join = analyze_results(df_join)

    df_join.to_csv("../../data/interim/df_join.csv")

    order_cols = ["BranchCode", "CardName", "ItemCode", "ItemName", "UoMGroup",
                  "ItemGroupName", "StoreClassification", "Leadtime", "MDQ",
                  "Ave. Daily Sales offtake", "month_end_inventory", "days_to_sell", "suggested_po",
                  "conversion", "UOM", "Qty", "UnitCost", "total_amount_order", "sales_offtake"]
    agg_group = {"ItemName":"unique", "UoMGroup":"unique", "ItemGroupName":"unique",
                 "TotalSRP":"max", "StoreClassification":"unique", "Leadtime":"max",
                 "MDQ":"max", "UOM":"unique", "UnitCost":"sum", "sales_offtake":"sum",
                 "month_end_inventory":"sum"}
    df_group = (df_join.groupby(["ItemCode"]).agg(agg_group).reset_index())

    df_group = analyze_results(df_group)

    path = "../../data/processed/po_analysis.xlsx"
    df_header, df_line = add_transaction_data(df_join)
    with pd.ExcelWriter(path) as writer:
        df_group[order_cols[2:]].to_excel(writer, sheet_name="groupby_plu_po_analysis", index=False)
        df_join[order_cols].to_excel(writer, sheet_name="branch_po_analysis", index=False)

    branches = df_header[['BranchCode', "DocNumber"]].values.tolist()
    # print(branches)
    today = date.today().strftime("%m-%d-%Y")
    for row in branches:
        branch = row[0]
        doc_number = row[1]
        path = ("../../data/processed/transaction/po_" + branch + "_" +
                today + "_" + doc_number + ".xlsx")
        with pd.ExcelWriter(path) as writer:
            (df_header[df_header['BranchCode'] == branch]
             .to_excel(writer, sheet_name="TransactionHeader", index=False))
            (df_line[df_line['BranchCode'] == branch]
             .to_excel(writer, sheet_name="TransactionLine", index=False))

main()
