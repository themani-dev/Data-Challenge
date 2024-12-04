import pandas as pd
import duckdb


# Data Initialization
projectsDf = pd.read_excel("Data Engineer Data Challenge.xlsx",sheet_name="Project")
milestoneDf = pd.read_excel("Data Engineer Data Challenge.xlsx",sheet_name="Milestone")
BudgetDf = pd.read_excel("Data Engineer Data Challenge.xlsx",sheet_name="Project_Budget")
CommitmentDf = pd.read_excel("Data Engineer Data Challenge.xlsx",sheet_name="Project_Committed")

DashboardLedgerKPIDf = pd.read_excel("Dashboard_Dump/Budget Ledgar.xlsx",sheet_name=0,header=2)
DashboardBudgetOverviewKPIDf = pd.read_excel("Dashboard_Dump/Budget Overview.xlsx",sheet_name=0,header=2)
DashboardProjectCountKPIDf = pd.read_excel("Dashboard_Dump/Project Count.xlsx",sheet_name=0,header=2)
DashboardTotalBudgetKPIDf = pd.read_excel("Dashboard_Dump/Total Budget.xlsx",sheet_name=0,header=2)

print(DashboardTotalBudgetKPIDf)

# Data Preprocessing
TransformedBudgetDf = duckdb.query(
    "select Project_ID,\
        Scope_ID,\
        SUBSTRING(Discipline, 5,LENGTH(DISCIPLINE)) as DIVISION,\
        String_AGG(Scope,', ') as Scope, \
        sum(Approved_Budget) as Approved_Budget,\
        Change_Date\
    from BudgetDf \
    group by Project_ID,Scope_ID,DIVISION,Change_Date"
).df()

TransformedCommittedDf = duckdb.query(
    "SELECT Scope_ID,sum(Committed) as Committed from CommitmentDf group by Scope_ID"
).df()


# KPI Build from Source file
LedgerKPIDf = duckdb.query(
    "SELECT pro.Project_Code as Project,bud.scope_ID, \
        bud.Approved_Budget,\
        comm.Committed \
    from TransformedBudgetDf bud \
    left join projectsDf pro \
    on pro.Project_ID = bud.Project_ID\
     left join TransformedCommittedDf comm on bud.Scope_ID = comm.Scope_ID"
).df()

BudgetOverviewKPIDf = duckdb.query(
    "select bd.Project_Id,prj.Project_CODE,sum(bd.Approved_Budget) as Approved_Budget,sum(com.Committed) as Committed \
    from TransformedBudgetDf bd left join TransformedCommittedDf com on bd.Scope_ID = com.Scope_ID \
    inner join projectsDf prj on prj.Project_ID = bd.Project_ID group by bd.Project_ID, prj.Project_Code "
).df()

# Validation
LedgarKPIResultDf = duckdb.query(
    'select core.*,abs(Approved_Budget - Dashboard_Approved_Budget) as Approved_Budget_Difference,abs(Committed - Dashboard_Committed) as Committed_Budget_Difference  from (Select t1.*,t2.Committed as Dashboard_Committed,\
        t2.Approved_Budget as Dashboard_Approved_Budget \
    from LedgerKPIDf t1 \
    inner join DashboardLedgerKPIDf t2 \
    on t1.Project = t2.Project_Code and t1.Scope_ID = t2.Scope_ID) core'
).df()

BudgetOverviewKPIResultdf = duckdb.query(
    'select core.*,\
        abs(core.Approved_Budget-Dashboard_Approved_Budget) as Approved_Budget_Diff, \
        abs(core.Committed-Dashboard_Committed) as Committed_Diff from \
    ( select t1.Project_Code,round(t1.Approved_Budget,2) as Approved_Budget,t2."Sum of Approved_Budget" as Dashboard_Approved_Budget,round(t1.Committed,2) as Committed,t2.Committed as Dashboard_Committed from BudgetOverviewKPIDf t1 \
    inner join DashboardBudgetOverviewKPIDf t2 on t1.Project_Code = t2.Project_Code) core'
).df()


# LedgarKPIResultDf.to_csv("Results/Ledgar_Result.csv",sep=',',index=False)
# BudgetOverviewKPIResultdf.to_csv("Results/Budget_Overview_Result.csv",sep=',',index=False)
