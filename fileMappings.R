
####################################################################################
#                                  File inputs                                     #   
####################################################################################
#                                                                                  #
# As input for every function use a list of dataframes corresponding to the output #
# of the KBC extractor. This should correspond to the mapping in the KBC transform #
#                                                                                  #
####################################################################################

require(readr)
require(dplyr)

## Sample input

#Adwords

## This is the query in the extractor: 

# SELECT   AdGroupId,
#          AdGroupName,
#          CampaignId,
#          CampaignName,
#          AllConversions,
#          AverageCost, 
#          Impressions, 
#          Cost,
#          Clicks, 
#          Date 
#  FROM ADGROUP_PERFORMANCE_REPORT

adw_adgroups<- read_csv("/data/in/tables/in.c-keboola-ex-adwords-v201705-239092703.ad_groups.csv")
adw_campaigns<-read_csv("/data/in/tables/in.c-keboola-ex-adwords-v201705-239092703.campaigns.csv")%>%dplyr::rename(Campaign_ID=id)
adw_customers<- read_csv("/data/in/tables/in.c-keboola-ex-adwords-v201705-239092703.customers.csv")%>%dplyr::rename(customerName=name)

adwords<-list(adw_adgroups,adw_campaigns,adw_customers)

#Sklik

skl_accounts <- read_csv("/data/in/tables/out.c-sklik.accounts.csv")%>%dplyr::rename(accountId=userId)
skl_campaigns <- read_csv("/data/in/tables/out.c-sklik.campaigns.csv")%>%dplyr::rename(campaignId=id)
skl_stats <- read_csv("/data/in/tables/out.c-sklik.stats.csv")%>%select(-accountId)

sklik<-list(skl_stats,skl_campaigns,skl_accounts)


#Fb Ads

fb_ads_accounts<-read_csv("./data/in/tables/in.c-keboola-ex-facebook-ads-275982876.accounts.csv")%>%rename(account_name=name)
fb_ads_campaigns<-read_csv("./data/in/tables/in.c-keboola-ex-facebook-ads-275982876.campaigns.csv")%>%rename(campaign_id=id,campaign_name=name)
fb_ads_adsets<-read_csv("./data/in/tables/in.c-keboola-ex-facebook-ads-275982876.adsets.csv")%>%rename(adset_id=id,adset_name=name)
fb_ads_ads<-read_csv("./data/in/tables/in.c-keboola-ex-facebook-ads-275982876.ads.csv")%>%rename(ad_id=id,ad_name=name)
fb_ads_insights<-read_csv("./data/in/tables/in.c-keboola-ex-facebook-ads-275982876.ads_insights.csv")

fb_ads<-list(fb_ads_insights,fb_ads_ads,fb_ads_adsets,fb_ads_campaigns,fb_ads_accounts)


#Google Analytics

# Used configuration

# Metrics
# ga:visits(Sessions), ga:goalCompletionsAll(Goal Completions), ga:goalValueAll(Goal Value), ga:users(Users)
# 
# Dimensions
# ga:date(Date), ga:source(Source), ga:medium(Medium), ga:campaign(Campaign), ga:adwordsCampaignID(AdWords Campaign ID), ga:adMatchType(Query Match Type), ga:adTargetingType(Targeting Type), ga:adDistributionNetwork(Ad Distribution Network)

ga_profiles<-read_csv("/data/in/tables/in.c-keboola-ex-google-analytics-v4-239094119.profiles.csv")%>%rename(idProfile=id)
ga_stats<-read_csv("/data/in/tables/in.c-keboola-ex-google-analytics-v4-239094119.all_ga_profiles.csv")

ga<-list(ga_stats,ga_profiles)

## List all the files in one list

all_data<-list(ga=ga,fb_ads=fb_ads,sklik=sklik,adwords=adwords)

####################################################################################
#                                  File inputs                                     #   
####################################################################################
#                                                                                  #
# As input for every function use a list of dataframes corresponding to the output #
# of the KBC extractor. This should correspond to the mapping in the KBC transform #
#                                                                                  #
####################################################################################

clean<-bulk_clean(all_data)
norm<-bulk_clean(all_data,method="normalize")

tbl_names<-paste0("./data/out/tables/",names(clean),".csv")

#write all tables as csv in the out bucket
map2(clean,tbl_names,function(x,y){write_csv(x,y)})

#write the out campaign table. 
write_csv(norm,"./data/out/tables/campaigns.csv")

