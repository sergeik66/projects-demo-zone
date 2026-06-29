self.data_quality_enabled = config["curatedProperties"].get("dataQualityEnabled", True)

        # === DQ Observability Metrics ===
        self.ALLMETRICS.dq_enabled = self.data_quality_enabled
        self.ALLMETRICS.dq_executed = False
        self.ALLMETRICS.dq_status = "skipped" if not self.data_quality_enabled else "pending"
        self.ALLMETRICS.expectations = None

        expectation_success = True

        if self.data_quality_enabled:
            dq = (
                Expectations(
                    self.target_lakehouse_name,
                    self.target_schema,
                    self.target_table,
                    self._source_data,
                    workspace_id=self._target_workspace_id,
                )
                .set_expectations(
                    self.target_table,
                    self.target_schema,
                    self.target_lakehouse_name
                )
            )
            if dq.expectations:
                self.logger.info("Running DQ expectations")
                dq.perform_expectations(self.candidate_keys)
                dq.quarantine(self.quarantine_strategy)
                self._source_data = dq.expectation_df.drop(
                    *[c for c in dq.expectation_df.columns if c.startswith("dq_")]
                )
                if dq.quarantine_metrics:
                    self.ALLMETRICS.quarantine.append(dq.quarantine_metrics)
                self.ALLMETRICS.expectations = dq.expectation_metrics
                expectation_success = dq.expectation_metrics.get("success", True)

                # Update observability status
                self.ALLMETRICS.dq_executed = True
                self.ALLMETRICS.dq_status = "passed" if expectation_success else "failed"
            else:
                self.logger.info("No expectations defined for this dataset")
                self.ALLMETRICS.dq_status = "no_expectations_defined"
                self.ALLMETRICS.dq_executed = False
        else:
            self.logger.info("Data quality expectations skipped (dataQualityEnabled=false in config)")
            self.ALLMETRICS.dq_status = "skipped"
            self.ALLMETRICS.dq_executed = False

        if expectation_success:
            self.logger.info(f"Load type: - {self.load_type} and target_exists - {target_exists}")
            # Execute load
            if self.load_type == "merge" and not target_exists:
                self._load_type.first_time_load(self._source_data)
            else:
                self._load_type.ingest(self._source_data)

            self.METRICS.success = True
            inserts, updates, deletes, output_bytes = self._load_type.get_table_metrics(table_path)
            self.METRICS.recordInserts = inserts
            self.METRICS.recordUpdates = updates
            self.METRICS.recordDeletes = deletes
            self.METRICS.outputBytes = output_bytes
        else:
            self.logger.warning("DQ expectations failed - skipping load (expectation_success=False)")
            self.METRICS.success = False
