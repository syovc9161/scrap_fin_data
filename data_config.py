from dataclasses import dataclass

@dataclass 
class Colconfig:
    bal_col = 'balanceSum'
    cnt_col = 'secncntSum'
    company_col = 'repSecnNm'

    dls_bal = ['balanceSumDls', 'dlsNsubsGuarBalance', 'dlsNsubsNguarBalance', 'dlsSubsGuarBalance','dlsSubsNguarBalance']
    els_bal = ['balanceSumEls', 'elsNsubsGuarBalance',  'elsNsubsNguarBalance', 'elsSubsGuarBalance', 'elsSubsNguarBalance']
    init_dls_bal = ['dlsNsubsGuarBalance', 'dlsNsubsNguarBalance', 'dlsSubsGuarBalance','dlsSubsNguarBalance']
    init_els_bal = ['elsNsubsGuarBalance',  'elsNsubsNguarBalance', 'elsSubsGuarBalance', 'elsSubsNguarBalance']

    dls_cnt = ['dlsNsubsGuarSecncnt', 'dlsNsubsNguarSecnCnt', 'dlsSubsGuarSecncnt', 'dlsSubsNguarSecncnt']
    els_cnt = ['elsNsubsNguarSecncnt', 'elsNsubsGuarSecncnt', 'elsSubsGuarSecncnt', 'elsSubsNguarSecncnt']

    numeric_cols = ['balanceSum', 'dlsNsubsGuarBalance', 'dlsNsubsGuarSecncnt', 'dlsNsubsNguarBalance', 'dlsNsubsNguarSecnCnt'
                  , 'dlsSubsGuarBalance', 'dlsSubsGuarSecncnt', 'dlsSubsNguarBalance', 'dlsSubsNguarSecncnt'
                  , 'elsNsubsGuarBalance', 'elsNsubsGuarSecncnt', 'elsNsubsNguarBalance', 'elsNsubsNguarSecncnt'
                  , 'elsSubsGuarBalance', 'elsSubsGuarSecncnt', 'elsSubsNguarBalance', 'elsSubsNguarSecncnt'
                  , 'secncntSum']
    date_cols = ['date']

    total_api_cols = ['balanceSum', 'dlsNsubsGuarBalance', 'dlsNsubsGuarSecncnt', 'dlsNsubsNguarBalance', 'dlsNsubsNguarSecnCnt'
                  , 'dlsSubsGuarBalance', 'dlsSubsGuarSecncnt', 'dlsSubsNguarBalance', 'dlsSubsNguarSecncnt'
                  , 'elsNsubsGuarBalance', 'elsNsubsGuarSecncnt', 'elsNsubsNguarBalance', 'elsNsubsNguarSecncnt'
                  , 'elsSubsGuarBalance', 'elsSubsGuarSecncnt', 'elsSubsNguarBalance', 'elsSubsNguarSecncnt'
                  , 'repSecnNm', 'secncntSum', 'date']
