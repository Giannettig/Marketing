####################################################################################
#                      GG Marketing R transformation library                       #   
####################################################################################
#                                                                                  #
# The aim of this library is to symplify the transformation and ingestion of       #
# marketing data using functional proframming in R.                                #
#                                                                                  #
# The idea is that there are several marketing tools providin information about    #
# impression, clicks, and cost. which need to be paired with Google analytics      #  
#                                                                                  #
# All the cleaning functions will return a clean orig table and a standartized     #
# table. There will be some tests performed inside to make sure no errors occurs   #
# and if yes It will return an appropriate error or warning.                       #
#                                                                                  #
####################################################################################


####################################################################################
#                                 Required libraries                               #   
####################################################################################

require(urltools)
require(digest)
require(tidyverse)

####################################################################################
#                                 Preprocessing functions                          #   
####################################################################################


#' NA Replace
#'
#' @param df 
#'
#' @return a dataframe where all the char, integer and double NA values are set to a default
#' @export
#'
#' @examples
#' 

NAreplace<-function(df){
  
  require(stringr)
  require(purrr)
  
  #Replacements
  df_types<-map_chr(df,function(x){ifelse(length(class(x))>1,"matrix",class(x))})
  
  df_replacements<-df_types%>%
                    lapply(function(x){
                      if(x=="character") as.character("")
                      else
                      if(x=="numeric") as.numeric(0)
                      else
                      if(x=="integer") as.integer(0)
                      else
                      "skip"
                    })
  
  #Detect all NAs in Dataframe and change them with replacement
  df<-map2_df(df,df_replacements,function(x,y){if(y=="skip") x else{x[is.na(x)]<-y ;x }})
  
}


#' Preprocess
#'
#' @param df_list - A list of dataframes
#'
#' @return Returns a result of joined dataframe and does some basic cleaning
#' @export
#'
#' @examples 

preprocess<-function(df_list){
  
  require(dplyr)
  require(purrr)
  
  # Join all the datasets using a common id key in sequence
  
  df<-reduce(df_list, left_join)
  
  # Clean the NA's
  df<-NAreplace(df)
  
}


####################################################################################
#                            Dataset specific functions                            #   
####################################################################################
#                                                                                  #
# All the functions in this section do basically the same:                         #
#                                                                                  #
# Join all the data from each source into one denormalized table.                  #
# Remove redundand rows (e.g where there is no impression, click, conversion or    #
# cost)                                                                            #
#                                                                                  #
# Create a unified table that can be rowbind with other marketing systems          #
# Gather client information for attributing costs and results to clients.          #
#                                                                                  #
# All the functions have a methos to check for errors. (duplicates etc)            #
#                                                                                  #
####################################################################################

##Sklik

clean_sklik<-function(df_list, method="clean"){
  
  if(class(df_list)!="list") stop("The input of this function should be a list of dataframes")

#Conversion to CZK and Percentages to decimals  

sklik_df<-preprocess(df_list)%>%
          mutate(impressionShare=impressionShare/100, 
                 ctr=ctr/100,
                 cpc=cpc/100,
                 price=price/100,
                 dayBudget=dayBudget/100
                 )

  if(method="normalize"){
    
  norm_df<-sklik_df%>%mutate(source="seznam",medium="cpc")%>%
                      select(date,name,medium,source,target,impressions,clicks,price,username,accountId)%>%
                      rename(campaign=name,cost=price,sklik_client=username,sklik_id=accountId)
    
  }else{
# return cleaned dataset
  return(sklik_df)}
  
}

##Adwords
clean_adwords<-function(df_list, method="clean"){
  
  if(class(df_list)!="list") stop("The input of this function should be a list of dataframes")

adwords_df<-preprocess(df_list)%>%mutate(Cost=Cost/1000000, Avg_Cost=Avg_Cost/1000000)

if(method="normalize"){
  
  norm_df<-adwords_df%>%mutate(medium="cpc",source="adwords")%>%
                        select(Day,Campaign,medium,source,advertisingChannelType,Impressions,Clicks,Cost,customerName,customerId)%>%
                        rename(campaign=Campaign,
                               cost=Cost,
                               date=Day,
                               impressions=Impressions,
                               clicks=Clicks,
                               adw_client=customerName,
                               adw_id=customerId,
                               target=advertisingChannelType)
  
}else{
  # return cleaned dataset
  return(adwords_df)}

}

##Facebook Ads 

clean_fb_ads<-function(df_list, method="clean"){
  
  if(class(df_list)!="list") stop("The input of this function should be a list of dataframes")
  
 # get rid of redundand columns
  df_list<-map(df_list,select,-dplyr::contains("ex_account_id"),-dplyr::contains("parent_id"),-dplyr::contains("fb_graph_node"))
  
  fb_ads_df<-preprocess(df_list)
  
  if(method="normalize"){
    
    norm_df<-fb_ads_df%>%mutate(medium="facebookads",source="facebook", target="context")%>%
      select(date_start,campaign_name,medium,source,target,impressions,clicks,spend,account_name,account_id)%>%
      rename(campaign=campaign_name,
             cost=spend,
             fb_client=account_name,
             fb_id=account_id)
    
  }else{
    # return cleaned dataset
    return(fb_ads_df)}
  
  
  
}

##Google Analytics

clean_ga<-function(df_list, method="clean"){
  
  if(class(df_list)!="list") stop("The input of this function should be a list of dataframes")
  
  ga_df<-preprocess(df_list)
  
  if(method="normalize"){
    
    norm_df<-ga_df%>%
      select(date,campaign,medium,source,adDistributionNetwork,visits,users,goalCompletionsAll,webPropertyName,webPropertyId)%>%
      rename(conversions=goalCompletionsAll,
             ga_client=webPropertyName,
             ga_id=webPropertyId)
    
  }else{
    # return cleaned dataset
    return(ga_df)}
  
}
