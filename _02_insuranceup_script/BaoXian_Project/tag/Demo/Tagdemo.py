from tag.base.SaveToMysql import Save_to_Mysql
from tag.claim_info import BuyTime,Claimitem,Claimmnt,Claimdate
from tag.policy_benefit import Age_buyChild,buy_datetimeChild,insur_codeChild,pol_flagChild,pppChild
from tag.policy_client import birthChild,CityChild,DirectionChild,eduChild,\
    heightChild,incomeChild,MarriageChild,ProviceChild,raceChild,SexTag,signChild
from tag.policy_surrender import Keeptime

if __name__ == '__main__':

    # policy_client
    sex1 = SexTag.sex("sexTask", 8)
    sex1.execute(True)
    birth = birthChild.BirthChild('birthTask', 19)
    birth.execute()
    City = CityChild.Citychild('cityTask', 62)
    City.execute()
    Direction = DirectionChild.Direction('cityTask', 522)
    Direction.execute()
    Edu = eduChild.Educhild('eduTask', 549)
    Edu.execute()
    sexModel = heightChild.height('heightTask', 11)
    sexModel.execute()
    Income = incomeChild.IncomeModel('IncomeTask', 530)
    Income.execute()
    Marriage1 = MarriageChild.Marriage('MarriageTask', 545)
    Marriage1.execute()
    province = ProviceChild.Provincechild('proviceTask', 29)
    province.execute()
    Race = raceChild.Racechild('raceTask', 537)
    Race.execute()
    Sign = signChild.Signchild('raceModel', 557)
    Sign.execute()


    # claim_info
    buytime1 = BuyTime.buytime("buytimeTask", 604)
    buytime1.execute()
    cliam_date = Claimdate.claim_date('ClaimdateTask', 614)
    cliam_date.execute()
    Claim_item = Claimitem.Claim_itemModel('ClaimitemTask', 624)
    Claim_item.execute()
    Claim_mnt = Claimmnt.Claim_mntModel('ClaimmntTask', 630)
    Claim_mnt.execute()

    # policy_benefit
    Age_buyModel = Age_buyChild.Age_Buy('Age_buyTask', 575)
    Age_buyModel.execute()
    BuyCycles = buy_datetimeChild.BuyCycle('BuyCycleModel', 604)
    BuyCycles.execute()
    Insur_code = insur_codeChild.Insur_codeModel('InsurcodeTask', 595)
    Insur_code.execute()
    Pol_flag = pol_flagChild.Pol_flagModel('Pol_flagTask', 601)
    Pol_flag.execute()
    PPP = pppChild.Pppchild('PPPModel', 570)
    PPP.execute()

    # policy_surrender
    pol_dates = Keeptime.PolDates('pol_datesModel', 634)
    pol_dates.execute()

    Save_to_Mysql(mysqlname="htv_rule.htv_result",user="root",password=123456)