self.data_quality_enabled = config["curatedProperties"].get("dataQualityEnabled", True)

expectation_success = True
        self.ALLMETRICS.expectations = None

        if self.data_quality_enabled:
            dq = (
                Expectations(
                    self.target_lakehouse_name,
                    self.target_schema,
                    self.target_table,
                    self._source_data,
                    workspace_id=self._target_workspace_id,   # from previous cross-workspace fix
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
            else:
                self.logger.info("No expectations defined for this dataset")
        else:
            self.logger.info("Data quality expectations skipped (dataQualityEnabled=false in config)")

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
