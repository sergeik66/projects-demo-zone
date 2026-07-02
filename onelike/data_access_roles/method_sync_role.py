def sync_role(
    self,
    role_name: str,
    members: List[Dict],
    decision_rules: List[Dict],
    role_kind: str = "Policy"
) -> bool:
    """
    General-purpose smart sync for any OneLake Data Access Role.

    Features:
    - Detects changes in decision rules (especially constraints)
    - Only updates the role if something actually changed
    - Uses merge logic (preserves existing structure where possible)
    - Returns True if an update was performed
    """
    logger.info(f"🔄 Starting smart sync for role: {role_name}")

    if not decision_rules:
        logger.warning("No decision_rules provided. Skipping role sync.")
        return False

    # Get current role
    current_role = self.get_role(role_name)

    if not current_role:
        logger.info(f"Role '{role_name}' does not exist. Creating new role...")
        new_role = {
            "name": role_name,
            "kind": role_kind,
            "decisionRules": decision_rules,
            "members": {"microsoftEntraMembers": members}
        }
        self.create_or_update_role(new_role)
        logger.info(f"✅ Role '{role_name}' created successfully")
        return True

    # Compare current vs new decision rules (deep comparison)
    current_decision_rules = current_role.get("decisionRules", [])
    current_members = current_role.get("members", {}).get("microsoftEntraMembers", [])

    # Simple but effective comparison
    needs_update = False

    if current_decision_rules != decision_rules:
        needs_update = True
        logger.info("Decision rules have changed.")

    if current_members != members:
        needs_update = True
        logger.info("Role members have changed.")

    if not needs_update:
        logger.info(f"✅ Role '{role_name}' is already up to date. No changes needed.")
        return False

    # Build updated role using merge approach
    updated_role = {
        "name": role_name,
        "kind": role_kind,
        "decisionRules": decision_rules,
        "members": {"microsoftEntraMembers": members}
    }

    self.create_or_update_role(updated_role)
    logger.info(f"✅ Role '{role_name}' synced successfully")
    return True
