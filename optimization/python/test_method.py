# ------------------------------------------------------------------ #
    # Minimal Test Helper (use this first)
    # ------------------------------------------------------------------ #
    def create_minimal_test_role(self, role_name: str = "TestRole_Minimal"):
        """Creates the simplest possible role to test if basic creation works."""
        role = {
            "name": role_name,
            "kind": "Policy",
            "decisionRules": [{
                "effect": "Permit",
                "permission": [
                    {"attributeName": "Path", "attributeValueIncludedIn": ["*"]},
                    {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
                ]
            }]
        }
        print(f"\n>>> Creating minimal test role: {role_name}")
        return self.create_or_update_role(role)

# === STEP 1: Test with minimal role first ===
    try:
        result = manager.create_minimal_test_role("TestRole_Debug")
        print("Minimal role created successfully!")
    except Exception as e:
        print("Minimal role failed:", e)

    # === STEP 2: Then try your real role ===
    # result = manager.ensure_pii_full_access_role(members=members)
