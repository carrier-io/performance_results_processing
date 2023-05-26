total_time <- Sys.time()
library("data.table")
library("readr")
library(lubridate);
library(dplyr)
library('R.utils')
library(httr)
library(glue)
library(jsonlite)

build_id = Sys.getenv("build_id")
base_url = Sys.getenv("base_url")
project_id = Sys.getenv("project_id")
bucket = Sys.getenv("bucket")
token = Sys.getenv("token")
s3_integration = fromJSON(Sys.getenv("integrations"))$system$s3_integration

aggregate_results <- function(original_results_csv, aggregation, aggregation_suffix) {
  lts <- Sys.time()
  print(aggregation_suffix)
  aggregatedCSV <- mutate(original_results_csv, interval=as.integer(time) %/% aggregation )
  difftime(Sys.time(), lts)
  results <- aggregatedCSV %>%
  dplyr::group_by(request_name = aggregatedCSV$request_name, method = aggregatedCSV$method, status = aggregatedCSV$status, group = aggregatedCSV$interval) %>%
  dplyr::summarize(time=glue("{gsub(' ', 'T', first(time))}Z"),
                   total=n(),
                   min=min(response_time),
                   max=max(response_time),
				   median=as.integer(quantile(response_time, c(.50))),
				   pct90=as.integer(quantile(response_time, c(.90))),
                   pct95=as.integer(quantile(response_time, c(.95))),
                   pct99=as.integer(quantile(response_time, c(.99))),
                   "1xx"=sum(startsWith(as.character(status_code), "1")),
                   "2xx"=sum(startsWith(as.character(status_code), "2")),
                   "3xx"=sum(startsWith(as.character(status_code), "3")),
                   "4xx"=sum(startsWith(as.character(status_code), "4")),
                   "5xx"=sum(startsWith(as.character(status_code), "5")),
                   "NaN"=sum(is.nan(status_code)))

  results = results[,c(5,1,2,3,6,7,8,9,10,11,12,13,14,15,16,17,18)]
  file_name = glue("/tmp/{build_id}_{aggregation_suffix}.csv")
  write.csv(results, file_name, row.names = FALSE, fileEncoding = "UTF-8", quote = FALSE, eol = "\n")
  gzip(file_name, destname=glue("{file_name}.gz"))
  url = glue("{base_url}/api/v1/artifacts/artifacts/{project_id}/{bucket}")
  r = POST(url, body = list("file" = upload_file(glue("{file_name}.gz"))), query = s3_integration, 
           add_headers("Authorization" = glue("Bearer {token}")))
  rm(results)
  difftime(Sys.time(), lts)
}

aggregate_users <- function(original_users_csv, aggregation, aggregation_suffix) {
  aggregatedCSV <- mutate(original_users_csv, interval=as.integer(time) %/% aggregation )
  results <- aggregatedCSV %>%
  dplyr::group_by(group = aggregatedCSV$interval) %>%
  dplyr::summarize(time=glue("{gsub(' ', 'T', first(time))}Z"),
                   sum=sum(tapply(active, lg_id, max)))

  results = results[,c(2,3)]
  file_name = glue("/tmp/users_{build_id}_{aggregation_suffix}.csv")
  write.csv(results, file_name, row.names = FALSE, fileEncoding = "UTF-8", quote = FALSE, eol = "\n")
  gzip(file_name, destname=glue("{file_name}.gz"))
  url = glue("{base_url}/api/v1/artifacts/artifacts/{project_id}/{bucket}")
  r = POST(url, body = list("file" = upload_file(glue("{file_name}.gz"))), query = s3_integration, 
           add_headers("Authorization" = glue("Bearer {token}")))
  rm(results)
}

get_response_times <- function(original_results_csv) {
  results <- original_results_csv %>%
  dplyr::summarize(min=min(response_time),
                   max=max(response_time),
                   mean=as.integer(mean(response_time)),
				   pct50=as.integer(quantile(response_time, c(.50))),
				   pct75=as.integer(quantile(response_time, c(.75))),
				   pct90=as.integer(quantile(response_time, c(.90))),
                   pct95=as.integer(quantile(response_time, c(.95))),
                   pct99=as.integer(quantile(response_time, c(.99))))

  file_name = glue("/tmp/response_times.csv")
  write.csv(results, file_name, row.names = FALSE, fileEncoding = "UTF-8", quote = FALSE, eol = "\n")
  rm(results)
}

get_comparison_data <- function(original_results_csv) {
  results <- original_results_csv %>%
  dplyr::group_by(request_name = original_results_csv$request_name, method = original_results_csv$method) %>%
  dplyr::summarize(total=n(),
                   ok=sum(tolower(status) == "ok"),
                   ko=sum(tolower(status) == "ko"),
                   min=min(response_time),
                   max=max(response_time),
                   mean=as.integer(mean(response_time)),
				   pct50=as.integer(quantile(response_time, c(.50))),
				   pct75=as.integer(quantile(response_time, c(.75))),
				   pct90=as.integer(quantile(response_time, c(.90))),
                   pct95=as.integer(quantile(response_time, c(.95))),
                   pct99=as.integer(quantile(response_time, c(.99))),
                   "1xx"=sum(startsWith(as.character(status_code), "1")),
                   "2xx"=sum(startsWith(as.character(status_code), "2")),
                   "3xx"=sum(startsWith(as.character(status_code), "3")),
                   "4xx"=sum(startsWith(as.character(status_code), "4")),
                   "5xx"=sum(startsWith(as.character(status_code), "5")),
                   "NaN"=sum(is.nan(status_code)))

  file_name = glue("/tmp/comparison.csv")
  write.csv(results, file_name, row.names = FALSE, fileEncoding = "UTF-8", quote = FALSE, eol = "\n")
  rm(results)
}

print("read results --------->")
results_csv_name = glue("/tmp/{build_id}.csv")
ts <- Sys.time()
original_results_csv <- fread(results_csv_name, select = c("time", "request_name", "method", "response_time", "status", "status_code"))
difftime(Sys.time(), ts)
records_count = nrow(original_results_csv)

ts <- Sys.time()

aggregate_results(original_results_csv, 600, "10m")
aggregate_results(original_results_csv, 300, "5m")
aggregate_results(original_results_csv, 60, "1m")
if (records_count < 100000000) {
  aggregate_results(original_results_csv, 30, "30s")
}
if (records_count < 50000000) {
  aggregate_results(original_results_csv, 5, "5s")
}
if (records_count < 5000000) {
  aggregate_results(original_results_csv, 1, "1s")
}

difftime(Sys.time(), ts)


get_response_times(original_results_csv)
get_comparison_data(original_results_csv)
rm(original_results_csv)

print("Read users ----------->")
users_csv_name = glue("/tmp/users_{build_id}.csv")
ts <- Sys.time()
original_users_csv <- fread(users_csv_name, select = c("time", "active", "lg_id"))
difftime(Sys.time(), ts)

print("aggregate_users -------->")
ts <- Sys.time()
if (records_count < 100000000) {
  aggregate_users(original_users_csv, 1, "1s")
}
if (records_count < 50000000) {
  aggregate_users(original_users_csv, 5, "5s")
}
if (records_count < 5000000) {
  aggregate_users(original_users_csv, 30, "30s")
}
aggregate_users(original_users_csv, 60, "1m")
aggregate_users(original_users_csv, 300, "5m")
aggregate_users(original_users_csv, 600, "10m")
difftime(Sys.time(), ts)
rm(original_users_csv)

print("Total time --------->")
difftime(Sys.time(), total_time)
