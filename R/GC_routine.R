#' @import data.table
#' @import rGBAT
#' @import rUSPS
#' @import rNYCclean

options(scipen = 999)

GC_routine <- function(in_clus, in_df, id_colname, hse_num_col_name=NULL, addr_col_name, third_col_name, source_cols, geocode_fields, addr_type, GBAT_name, gc_type, results_df=NULL, filter_col_name=NULL, filter_vector=NULL, split=FALSE){

	my_df <- copy(in_df)

	if(nrow(my_df)>0){
	
		if (!(is.null(filter_col_name))){
			new.geocode_fields <- unique(c(geocode_fields,filter_col_name))
		} else{
			new.geocode_fields <- geocode_fields
		}
	
	
		###split freeform address###
		if(split & is.null(hse_num_col_name)){
			hse_num_col_name <- "temp_hse_num"
			my_df[,(hse_num_col_name) := gsub("^(\\S+)\\s.*$","\\1",get(addr_col_name))]
			my_df[,(addr_col_name) := gsub("^\\S+\\s(.*)$","\\1",get(addr_col_name))]
		}

		if (!(is.null(hse_num_col_name))){
			###geocode two column addresses###
			results1_df <- parallel.GBAT.process_split_addresses(in_clus, my_df, hse_num_col_name, addr_col_name, third_col_name, source_cols, new.geocode_fields, addr_type, return_type = "all", GBAT_name)
		} else{
			###geocode freeform addresses###
			results1_df <- parallel.GBAT.process_freeform_addresses(in_clus, my_df, addr_col_name, third_col_name, source_cols, new.geocode_fields, addr_type, return_type = "all", GBAT_name)
		}
		
		results1_df[,pass := 1]
		
		if (!(is.null(filter_col_name))){
		
			results1_df[,pass := ifelse(get(filter_col_name) == "",0,pass)]
			
			if (!(is.null(filter_vector))){
				results1_df[,pass := ifelse(get(filter_col_name) %in% filter_vector, 0, pass)]
			}
		}
		
		###grab first record when there are duplicates###
		results1_df[,temp_id := .I]
		
		setorderv(results1_df, c(id_colname, "temp_id"), c(1, 1))
		
		results1_df[, rep.id := seq_len(.N), by=id_colname]
		
		results1_df <- results1_df[rep.id==1, !(names(results1_df) %in% c("rep.id","temp_id")), with=FALSE]
		
		###create bank of rejects###
		rejects_df <- results1_df[pass==0, names(results1_df) != "pass", with=FALSE]
		
		results1_df <- results1_df[pass==1, names(results1_df) != "pass", with=FALSE]
		
		if(nrow(results1_df)>0){
			
			results1_df[,GC.type := gc_type]
			
			if (!(is.null(results_df))){
				results1_df <- results1_df[!(get(id_colname) %in% results_df[,get(id_colname)])]
				p_num <- nrow(results1_df)
				results_df <- data.table::rbindlist(list(results1_df,results_df), use.names=TRUE, fill=TRUE)
			} else {
				results_df <- results1_df
				p_num <- nrow(results_df)
			}
			
		} else {
			p_num <- nrow(results1_df)
		}
		bul_str <- "\xE2\x80\xA2"
		Encoding(bul_str) <- "UTF-8"
		
		message_df <- data.table(num_rec=c(p_num), desc=c(gc_type), error=c(0))
		
		cat(paste0(bul_str," ", formatC(p_num, big.mark=",",format="d"), " records successfully geocoded by ", gc_type, ".\n"))
				
		invisible(gc())
	} else{
		message_df <- data.table(num_rec=c(0), desc=c(gc_type), error=c(0))
		cat(paste0("\tALERT... EMPTY DATA FRAME SUBMITTED!\n"))
		rejects_df <- NULL
	}
	
	return(list('results_df'=results_df,'rejects_df'=rejects_df,'message_df'=message_df))
}
