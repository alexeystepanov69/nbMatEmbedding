import os
from pathlib import Path

import click
import pandas as pd
import pyodbc as db

from linear_model import PricePrediction

connStr = 'Driver=SQL Server;Server=inm-srv-olp-db1;Database=Olimp_neftemash;Trusted connection=Yes;'

sqlCmd = """
select mw.fKey, c0.fName as MainCL, c1.fKey as ChClKey, c1.fName as ChildCL, mw.fMatName, mw.fProdFlag, b.LastDate
		, case when c0.fKey in (20, 21, 23) 
		        and exists(select 1 from spDSE d where d.f_spDSE_Types_3663 in (1, 2, 3, 6)
				and d.fOboz is not null and len(d.fOboz) >= 5 
				and mw.fMatName like '%' + replace(d.fOboz, ' ', '') + '%') then 1 else 0 end as fProprietary
		, m0.fKey as markCLKey, m0.fMark as fMarkCL, mm.fKey as markKey, mm.fMark
		, sz.fSize, mw.fPriceLast, mw.fPriceExpect 
		, dbo.GetRelEd2Ed_fn(mw.fKey, mw.f_spEd, 2) as fCoef 
		, mw.fPriceLast / dbo.GetRelEd2Ed_fn(mw.fKey, mw.f_spEd, 2) as Price_Kg
	from nbMat_View mw
	join nbMatCL c1 on mw.f_nbMatCL = c1.fKey
	join nbMatCL c0 on c1.f_nbMatCL = c0.fKey
	join (
			select ns.f_nbMat, max(n.fDocDateHeader) as LastDate
				from fbNaklad n
				join fbNaklad_Spec ns on ns.f_fbNaklad = n.fKey
				where n.fDocDateHeader >= ?
				group by ns.f_nbMat
			) b on mw.fKey = b.f_nbMat
	left join nbMatMark mm on mw.f_nbMatMark = mm.fKey
	left join nbMatMark m0 on mm.f_nbMatMark = m0.fKey
	left join (select m.fKey as KeyMat, nullif(max(ms.fSize), 0) as fSize
					from nbMat m 
					join nbMat_Size ms on ms.fKeyMat = m.fKey
					join nbMatSizeDef msd on ms.fNum = msd.fNum
					where msd.fKeyMatType = m.f_nbMatTypes
						and msd.fName in ('Толщина', 'Диаметр')
					group by m.fKey
					) sz on mw.fKey = sz.KeyMat
	order by 1, 2, 3
"""

sqlCmd2 = """
select a.f_nbMat, fCena * dbo.GetRelEd2Ed_fn(a.f_nbMat, a.f_spEd, m.f_spEd)/(1 + isnull(fNDS, 0)/100.0) 
from (
select f_nbMat, ROW_NUMBER() over(partition by f_nbMat order by zz.fDocDateHeader desc) as rwn, tbl.f_spEd, zz.fDocDateHeader, 
		tbl.fNDS, tbl.fWithoutNDS, tbl.fCena, tbl.fCenaCur, isnull(dog.f_spCur, 34) as f_spCur
	from pzReqZakTabl tbl
	join pzReqZakSpec zz on tbl.f_pzReqZakSpec = zz.fKey
	left join fbDogIn dog on zz.f_fbDogIn = dog.fKey
	where zz.fDocDateHeader >= '01.03.2022' and tbl.f_nbMat is not null
	) a
	join nbMat m on a.f_nbMat = m.fKey
	where a.rwn = 1
	"""

sqlCmd3 = """
select mw.fKey, c0.fName as MainCL, c1.fKey as ChClKey, c1.fName as ChildCL, mw.fMatName, isnull(mw.fProdFlag, 0) as fProdFlag, 
		isnull(b.LastDate, mw.fDateLast) as LastDate
		, case when c0.fKey in (20, 21, 23) 
		        and exists(select 1 from spDSE d where d.f_spDSE_Types_3663 in (1, 2, 3, 6)
				and d.fOboz is not null and len(d.fOboz) >= 5 
				and mw.fMatName like '%' + replace(d.fOboz, ' ', '') + '%') then 1 else 0 end as fProprietary
		, m0.fKey as markCLKey, m0.fMark as fMarkCL, mm.fKey as markKey, mm.fMark
		, sz.fSize, isnull(b.oldPrice, mw.fPriceLast) as fPriceLast, mw.fPriceExpect 
		, dbo.GetRelEd2Ed_fn(mw.fKey, mw.f_spEd, 2) as fCoef 
		, mw.fPriceLast / dbo.GetRelEd2Ed_fn(mw.fKey, mw.f_spEd, 2) as Price_Kg, new.newPrice
	from nbMat_View mw
	join nbMatCL c1 on mw.f_nbMatCL = c1.fKey
	join nbMatCL c0 on c1.f_nbMatCL = c0.fKey
	join (select a.f_nbMat, fCena * dbo.GetRelEd2Ed_fn(a.f_nbMat, a.f_spEd, m.f_spEd)/(1 + isnull(fNDS, 0)/100.0) as newPrice
					from (
					select f_nbMat, ROW_NUMBER() over(partition by f_nbMat order by zz.fDocDateHeader desc) as rwn, tbl.f_spEd, zz.fDocDateHeader, 
							tbl.fNDS, tbl.fWithoutNDS, tbl.fCena, tbl.fCenaCur, isnull(dog.f_spCur, 34) as f_spCur
						from pzReqZakTabl tbl
						join pzReqZakSpec zz on tbl.f_pzReqZakSpec = zz.fKey
						left join fbDogIn dog on zz.f_fbDogIn = dog.fKey
						where zz.fDocDateHeader >= ? and tbl.f_nbMat is not null
						) a
						join nbMat m on a.f_nbMat = m.fKey
						where a.rwn = 1 --and a.f_spCur = 34 
								and m.fDateLast is not null
				) new on new.f_nbMat = mw.fKey
	left join (
			select ns.f_nbMat, max(n.fDocDateHeader) as LastDate, max(ns.fPrice * dbo.GetRelEd2Ed_fn(ns.f_nbMat, ns.f_spEd, m.f_spEd)) as oldPrice
				from fbNaklad n
				join fbNaklad_Spec ns on ns.f_fbNaklad = n.fKey
				join nbMat m on ns.f_nbMat = m.fKey
				where n.fDocDateHeader >= '01.11.2021' and n.fDocDateHeader < '01.03.2022'
				group by ns.f_nbMat
			) b on mw.fKey = b.f_nbMat
	left join nbMatMark mm on mw.f_nbMatMark = mm.fKey
	left join nbMatMark m0 on mm.f_nbMatMark = m0.fKey
	left join (select m.fKey as KeyMat, nullif(max(ms.fSize), 0) as fSize
					from nbMat m 
					join nbMat_Size ms on ms.fKeyMat = m.fKey
					join nbMatSizeDef msd on ms.fNum = msd.fNum
					where msd.fKeyMatType = m.f_nbMatTypes
						and msd.fName in ('Толщина', 'Диаметр')
					group by m.fKey
					) sz on mw.fKey = sz.KeyMat
	where isnull(b.LastDate, mw.fDateLast) >= '01.09.2021'
	order by 1, 2, 3"""


def toBool(s: str) -> bool:
    try:
        if s is None:
            return False
        return bool(str)
    except:
        return False


@click.command()
@click.option('--workdir', '-w', default=os.environ.get('TMP', '/tmp'))
@click.option('--cache')
@click.option('--date-from', default='01.03.2022')
@click.option('--enforce-sql')
def main(workdir=None, cache=None, date_from=None, enforce_sql=None):
    print(f'Parameters: workdir={workdir}, cache={cache}, date_from={date_from}, enforce_sql={enforce_sql}')
    with db.connect(connStr) as conn:
        # df_price = pd.read_sql('exec fbNaklad_PricesApprecPerc', conn)
        df_price = pd.read_sql(sqlCmd2, conn)

    # df_for_train = df_price[df_price['PriceR'].notnull()]
    if cache is None or not Path(workdir).joinpath(cache).exists() or toBool(enforce_sql):
        with db.connect(connStr) as conn:
            df = pd.read_sql(sqlCmd3, conn, params=[date_from])
            if cache is not None:
                df.to_excel(Path(workdir).joinpath(cache), index=False)
    else:
        df = pd.read_excel(Path(workdir).joinpath(cache))

    print(f'price database consists of {len(df_price)} records')
    print(f'records = {len(df)}, columns = {df.columns}')

    lin_reg = PricePrediction(df)
    print(f'shape = {lin_reg.matrix.shape}, y_shape = {lin_reg.y.shape}')
    print(f'Score = {lin_reg.regression.score(lin_reg.matrix, lin_reg.y)}')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
