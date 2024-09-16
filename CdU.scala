import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
//import com.databricks.spark.avro._
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType, StringType, StructType, StructField,NullType}
import org.apache.spark.sql.functions.{substring, concat,when, lit,col,max, min, length, add_months, last_day, to_date, count, current_date, current_timestamp, _}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.functions.{col, date_format, when, _}
import org.apache.spark.sql.expressions.Window
import sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.sql.Date
spark.conf.set("spark.sql.parquet.writeLegacyFormat", true)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead","CORRECTED")
%%html
<style>
.output_stdout > pre{
    width:100000px !important;
}
</style>
%%html
<style>
div pre{
    white-space: pre !important;
}
</style>
val odate = "2022-06-15"
val dateFormat = "yyyy-MM-dd-HH.mm.ss.SSSSSS"
//"yyyy-MM-dd-HH mm.ss.SS"

val CargaMVR = spark.read.parquet("/data/master/mdco/data/t_mdco_receipts_dtmvr").filter(col("transaction_date") === odate)
                                                                .select(col("contract_entity_branch_id"), col("associated_contract_account_id"),
                                                                    col("gl_account_date"), col("movement_sequence_id"),col("user_audit_id"),
                                                                    col("settlement_date"),col("movement_capital_amount"), 
                                                                    col("transaction_date"), col("movement_interest_amount"),
                                                                    col("tax_over_interest_amount"), col("vat_commission_amount"),
                                                                    col("vat_email_expenses_amount"),col("vat_other_expenses_amount"), 
                                                                    col("vat_life_damage_insrnc_amount"), col("overdue_capital_interest_amount"),
                                                                    col("movement_commissions_amount"), col("mail_expenses_amount"), 
                                                                    col("other_expenses_amount"), col("insurance_expense_life_amount"), 
                                                                    col("damage_insurance_expns_amount")
                                                                    )

                                        
val CargaMOV = spark.read.parquet("/data/master/mdco/data/t_mdco_ugdtmov").filter(col("transaction_date") === odate)
                                                        .select(col("contract_entity_branch_id"),
                                                                col("associated_contract_account_id"),col("gl_account_date"),
                                                                col("movement_sequence_id"),col("concept_id"),col("transaction_date"),
                                                                col("user_audit_id"),col("movement_amount"),
                                                                                            col("concept_id")

                                                                
                                                                
                                                                
                                                        )
                                                        


val CargaMAE =  spark.read.parquet("/data/master/mdco/data/t_mdco_ugdtmae").filter(col("information_date") === odate)
                                                         .select(col("concatenated_entity_branch_id"),
                                                                 col("account_id"), col("currency_id")
                                                         )


val cargaTax = spark.read.parquet("/data/master/ktny/data/t_ktny_catalog_values_taxonomy")
                .filter(col("gf_frequency_type") === "D" && col("g_catalog_id") === "C362")

val mvrKey = CargaMVR.select(concat(lit("MX"),
                            substring(col("contract_entity_branch_id"), 1,4),
                            lit("14"),
                            substring(col("contract_entity_branch_id"), 5,9),
                            col("associated_contract_account_id"),
                            col("gl_account_date"),
                            col("movement_sequence_id"),
                            col("settlement_date")
                            ).as("g_movement_id"),
                        typedLit[Option[String]](None).cast("string").as("g_account_ref_move_id"),
                            col("transaction_date").as("gf_operation_date"),
                            col("movement_capital_amount"), col("movement_sequence_id"),col("user_audit_id"),
                            col("tax_over_interest_amount"), col("vat_commission_amount") , col("vat_email_expenses_amount"),
                            col("vat_other_expenses_amount") , col("vat_life_damage_insrnc_amount"),
                            col("vat_other_expenses_amount"), col("vat_life_damage_insrnc_amount"), col("movement_interest_amount"),
                            col("overdue_capital_interest_amount"), col("movement_commissions_amount"),col("mail_expenses_amount"),
                            col("other_expenses_amount"),col("insurance_expense_life_amount"), col("damage_insurance_expns_amount"),
                            col("contract_entity_branch_id"), col("associated_contract_account_id")
                            )
                                    
val mvrCapital = mvrKey.filter(col("movement_capital_amount")>0)
                        .select(col("g_movement_id"), lit("capital").as("g_detail_loan_type"), col("g_account_ref_move_id"),
                                col("movement_capital_amount").as("gf_detail_loan_amount"),col("gf_operation_date"),
                               col("contract_entity_branch_id"), col("associated_contract_account_id"),col("movement_sequence_id"),col("user_audit_id"))

val mvrTaxes = mvrKey.filter((col("tax_over_interest_amount") + col("vat_commission_amount") + col("vat_email_expenses_amount") +
                              col("vat_other_expenses_amount") + col("vat_life_damage_insrnc_amount")) > 0)

                    .select(col("g_movement_id"), lit("taxes").as("g_detail_loan_type"), col("g_account_ref_move_id"),
                            (col("tax_over_interest_amount") + col("vat_commission_amount") + col("vat_email_expenses_amount") +
                            col("vat_other_expenses_amount") + col("vat_life_damage_insrnc_amount")).as("gf_detail_loan_amount"),
                            col("gf_operation_date"),col("contract_entity_branch_id"),
                            col("associated_contract_account_id"),col("movement_sequence_id"),col("user_audit_id"))


val mvrInterest = mvrKey.filter((col("movement_interest_amount")+ col("overdue_capital_interest_amount")
                                )>0)
                        
                        .select(col("g_movement_id"), lit("interest").as("g_detail_loan_type"), col("g_account_ref_move_id"),
                            (col("movement_interest_amount")+ col("overdue_capital_interest_amount")).as("gf_detail_loan_amount"),
                            col("gf_operation_date"),col("contract_entity_branch_id"),
                            col("associated_contract_account_id"),col("movement_sequence_id"),col("user_audit_id"))

val mvrFeePaid = mvrKey.filter((col("movement_commissions_amount")
                                )>0)
                        
                        .select(col("g_movement_id"), lit("FeePaid").as("g_detail_loan_type"), col("g_account_ref_move_id"),
                            (col("movement_commissions_amount")).as("gf_detail_loan_amount"),
                            col("gf_operation_date"),col("contract_entity_branch_id"),
                            col("associated_contract_account_id"),col("movement_sequence_id"),col("user_audit_id"))

val mvrMailExp = mvrKey.filter((col("mail_expenses_amount")
                                )>0)
                        
                        .select(col("g_movement_id"), lit("Mail Exp").as("g_detail_loan_type"), col("g_account_ref_move_id"),
                            (col("mail_expenses_amount").as("gf_detail_loan_amount")), 
                            col("gf_operation_date"),col("contract_entity_branch_id"),
                            col("associated_contract_account_id"),col("movement_sequence_id"),col("user_audit_id"))


val mvrGenExp = mvrKey.filter((col("other_expenses_amount")+col("insurance_expense_life_amount")+
                            col("damage_insurance_expns_amount") )>0)
                        
                        .select(col("g_movement_id"), lit("Gen Exp").as("g_detail_loan_type"), col("g_account_ref_move_id"),
                            (col("other_expenses_amount")+col("insurance_expense_life_amount")+
                            col("damage_insurance_expns_amount")).as("gf_detail_loan_amount"),
                            col("gf_operation_date"),col("contract_entity_branch_id"),
                            col("associated_contract_account_id"),col("movement_sequence_id"),col("user_audit_id"))
val mvrCat = mvrGenExp.union(mvrMailExp.union(mvrFeePaid.union(mvrInterest.union(mvrTaxes.union(mvrCapital)))))

val MAEKey = CargaMAE.filter(col("information_date") === odate).select(
    col("concatenated_entity_branch_id"), col("account_id"), col("currency_id"))

val getCurrencyMvr = mvrCat.join(MAEKey,(mvrCat("contract_entity_branch_id") === MAEKey("concatenated_entity_branch_id"))&&
                                (mvrCat("associated_contract_account_id")===MAEKey("account_id")),"left"
                                )
                            .select(col("g_movement_id"), col("g_detail_loan_type"), col("g_account_ref_move_id"), 
                                    col("gf_detail_loan_amount"), col("gf_operation_date"),col("movement_sequence_id"),col("user_audit_id"),
                                    when(((MAEKey("account_id")).isNotNull && trim(col("currency_id"))==="MXP"), lit("MXN"))
                                    .when(((MAEKey("account_id")).isNotNull && trim(col("currency_id"))=!="MXP"), trim(col("currency_id")))
                                    .otherwise(typedLit[Option[String]](None).cast("string")).as("g_detail_Loan_currency_id"))

val MRVComplete = getCurrencyMvr.select(
         col("g_movement_id"),
         col("g_detail_loan_type"),
         col("g_account_ref_move_id"),
         typedLit[Option[String]](None).cast("string").as("g_banking_operation_id"),
         typedLit[Option[String]](None).cast("string").as("gf_local_account_contract_id"),
         col("gf_detail_loan_amount"),
         col("g_detail_loan_currency_id"),
         col("gf_operation_date"),
         to_timestamp(unix_timestamp(col("movement_sequence_id"),
                                                        dateFormat).cast("timestamp")).as("gf_opern_audit_insert_date"),
         //col("movement_sequence_id").cast("timestamp").as("gf_opern_audit_insert_date"),
         to_timestamp(unix_timestamp(col("movement_sequence_id"),
                                                        dateFormat).cast("timestamp")).as("gf_operational_audit_date"),
         //col("movement_sequence_id").cast("timestamp").as("gf_operational_audit_date"),
    col("gf_audit_operuser_id"),
         //col("user_audit_id").as("gf_audit_operuser_id"),
         typedLit[Option[String]](None).cast("string").as("gf_user_audit_id"),
         to_timestamp(unix_timestamp(col("movement_sequence_id"),
                                                        dateFormat).cast("timestamp")).as("gf_audit_date"),
         //col("movement_sequence_id").cast("timestamp").as("gf_audit_date"),
         current_date().cast("date").as("gf_cutoff_date")
        )
val movKey = CargaMOV.select(concat(lit("MX"),
                            substring(col("contract_entity_branch_id"), 1,4),
                            lit("54"),
                            substring(col("contract_entity_branch_id"), 5,9),
                            col("associated_contract_account_id"),
                            col("gl_account_date"),
                            col("movement_sequence_id"),
                            col("concept_id")
                            ).as("g_movement_id"),
                            col("associated_contract_account_id"),
                            col("contract_entity_branch_id"),
                            col("concept_id"),
                             
                            typedLit[Option[String]](None).cast("string").as("g_account_ref_move_id"),
                            typedLit[Option[String]](None).cast("string").as("g_banking_operation_id"),
                            typedLit[Option[String]](None).cast("string").as("gf_local_account_contract_id"),
                            col("movement_amount").as("gf_detail_loan_amount"),
                            col("transaction_date").as("gf_operation_date"),
                            to_timestamp(unix_timestamp(col("movement_sequence_id"),
                                                        dateFormat).cast("timestamp")).as("gf_opern_audit_insert_date"),

                            //col("movement_sequence_id").cast("timestamp").as("gf_opern_audit_insert_date"),
                            
                            to_timestamp(unix_timestamp(col("movement_sequence_id"),
                                                        dateFormat).cast("timestamp")).as("gf_operational_audit_date"),
                            //col("movement_sequence_id").cast("timestamp").as("gf_operational_audit_date"),
                            col("user_audit_id").as("gf_audit_operuser_id"),
                            typedLit[Option[String]](None).cast("string").as("gf_user_audit_id"),
                            to_timestamp(unix_timestamp(col("movement_sequence_id"),
                                                        dateFormat).cast("timestamp")).as("gf_audit_date"),
                            //col("movement_sequence_id").cast("timestamp").as("gf_audit_date"),
                            current_date().cast("date").as("gf_cutoff_date")
                             )

val TaxKey=cargaTax.select(col("g_catalog_id"),substring(col("gf_catalog_val_id"), 1,2).as("gf_catalog_val_id"),
                          //col("gf_catlg_field_value_es_desc"),
                           regexp_replace(col("gf_catlg_field_value_es_desc"), "[0-9]", "").as("gf_catlg_field_value_es_desc"))

TaxKey.select("gf_catlg_field_value_es_desc").distinct().show(55,false)

val MOVLoan = movKey.join(TaxKey,(movKey("concept_id") === TaxKey("gf_catalog_val_id")),"inner")

                            .select(col("g_movement_id"),col("gf_catlg_field_value_es_desc").as("g_detail_loan_type"),
                            col("contract_entity_branch_id"),col("associated_contract_account_id"),
                                    col("g_account_ref_move_id"),col("g_banking_operation_id"),
                                    col("gf_local_account_contract_id"),col("gf_detail_loan_amount"),
                                    col("gf_operation_date"),col("gf_opern_audit_insert_date"),
                                    col("gf_operational_audit_date"),col("gf_user_audit_id"),
                                    col("gf_audit_date"),col("gf_cutoff_date")

                                   )

val getCurrencyMov = MOVLoan.join(MAEKey,(MOVLoan("contract_entity_branch_id") === MAEKey("concatenated_entity_branch_id"))&&
                                (MOVLoan("associated_contract_account_id")===MAEKey("account_id")),"left"
                                )
                            .select(col("g_movement_id"),
                                    when(((MAEKey("account_id")).isNotNull && trim(col("currency_id"))==="MXP"), lit("MXN"))
                                    .when(((MAEKey("account_id")).isNotNull && trim(col("currency_id"))=!="MXP"), trim(col("currency_id")))
                                    .otherwise(typedLit[Option[String]](None).cast("string")).as("g_detail_Loan_currency_id"))

val MOVWithCurrency = MOVLoan.join(getCurrencyMov.select(col("g_movement_id"), col("g_detail_Loan_currency_id")).distinct(), Seq("g_movement_id"), "left").select(
    col("g_movement_id"),col("g_detail_loan_type"),
                                    col("g_account_ref_move_id"),col("g_banking_operation_id"),
                                    col("gf_local_account_contract_id"),col("gf_detail_loan_amount"),
                                    col("g_detail_Loan_currency_id"),
                                    col("gf_operation_date"),col("gf_opern_audit_insert_date"),
                                    //col("gf_operational_audit_date"),col("gf_user_audit_id"),
                                    col("gf_audit_date"),col("gf_cutoff_date")
                                   )
MRVComplete.union(MOVWithCurrency)
