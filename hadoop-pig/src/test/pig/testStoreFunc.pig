/**
 * Copyright (C) 2011 Metamarkets http://metamx.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
%declare input_path '/Users/nikhodgkinson/data/ft/dfp/facts/processed/NetworkImpression/y=2011/m=04/d=06/H=00/';
%declare output_path '/tmp/pig/store-func';

%declare impressions_schema 'timestamp:chararray, Time:chararray, User_ID:int, IP, Advertiser_ID:int, Order_ID:int, Ad_ID:int, Creative_ID:int, Creative_Version:int, Creative_Size_ID:chararray, Site_ID:int, Page_ID:int, Keyword:chararray, Country_ID:int, State_Province:chararray, Areacode, Browser_ID:int, Browser_Version:float, OS_ID:float, Domain_ID:int, DMA_ID:int, City_ID:int, Zip_Code:chararray, Connection_Type_ID:int, Site_Data:chararray, Time_UTC_Sec:int';

dfp_impressions = LOAD '$input_path' USING PigStorage() AS ($impressions_schema);

STORE dfp_impressions INTO '$output_path' USING com.metamx.milano.pig.MilanoStoreFunc();

/**/
