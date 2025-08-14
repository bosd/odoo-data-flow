# **Project Constitution: odoo-data-flow Next-Generation v3**

## **1\. Vision & Core Mission**

The primary goal is to evolve odoo-data-flow from a command-line utility into a robust, user-friendly, and Odoo-integrated data synchronization and transformation platform. The system must automate complex delta migrations, intelligently handle data conflicts, and empower functional users to manage data quality and flows directly from the Odoo user interface.  
The architecture should be forward-looking, creating a foundation that could eventually be extended to process data from other sources beyond Odoo databases, such as REST APIs or other external systems.

## **2\. Core Components & Architecture**

The system consists of two primary components that work in concert:

### **2.1. odoo-data-flow Python Package**

This is the core engine. It must be refactored to function as both a standalone Command-Line Interface (CLI) and an importable Python library.

* **Backend:** Must continue to leverage the polars library for all high-performance, in-memory data manipulation.  
* **CLI:** The CLI must support both orchestrated project flows and ad-hoc single actions.  
* **Library Mode:** The core logic must be callable from other Python applications, primarily the Odoo Orchestrator Module.

### **2.2. Odoo Orchestrator Module Suite**

This is the user-facing "mission control" center, living inside the Odoo environment. It will be composed of several smaller, interdependent modules.

* **odf\_core:** The base module. Manages connections, scheduling, pre-flight checks, and integration with the odoo-data-flow library.  
* **odf\_ui\_builder:** Provides the Odoo UI for users to visually create, order, and configure data flows and their transformation steps.  
* **odf\_data\_quality\_dashboard:** Manages post-import validation checks and presents data quality issues to users.  
* **odf\_conflict\_resolution:** Implements the UI and logic for the 3-Way Merge conflict resolution process.

## **3\. Architectural Principles**

* **Modularity:** Functionality must be broken down into the smallest logical modules possible.  
* **State Management:** State must be managed per-pipeline (source-destination pair).  
* **Hybrid Data Access:** Prioritize direct PostgreSQL reads for performance where possible, but always use the Odoo ORM for writes to ensure business logic is respected.  
* **Configuration in Database:** All user-facing configurations must be stored in Odoo models to avoid filesystem dependencies and permission issues.

## **4\. Feature Specifications**

### **4.1. Active Record Filtering**

* **Goal:** Allow users to filter out inactive records from the migration process with a manual override.  
* **Method:** Use an "Offline Analysis & Write-Back" strategy.  
  1. odoo-data-flow performs a lightweight analysis of transactional data to identify inactive records.  
  2. A new "update" flow connects back to the **source database (A)** and sets a boolean marker (e.g., x\_odf\_exclude\_migration \= True) on these inactive records.  
  3. Users in the source database can see this marker and un-check it to force inclusion.  
  4. The main export flow from A to B respects this marker, excluding any records where the flag is True.

### **4.2. Data Quality Dashboard**

* **Goal:** Manage data validation failures without blocking the import process.  
* **Method:** Import data quickly with expensive validations (e.g., VAT checks) disabled in the context. A separate, scheduled Odoo action runs these validations post-import and creates "Data Quality Issue" records for any failures. This model serves as a dashboard/work queue for the data cleaning team, **linking directly to the problematic record** for easy navigation and correction.

### **4.3. 3-Way Merge Conflict Resolution**

* **Goal:** Intelligently merge updates from the source without overwriting human cleaning work in the destination.  
* **Method:**  
  1. **Snapshotting:** Store a JSON snapshot of each record at the time of import in an x\_odf\_original\_snapshot field of type **JSONB** in the destination database. This is the "common ancestor."  
  2. **Merge Logic:** Compare Transformed Source, Current Destination, and Original Snapshot. Automatically apply "safe" updates where only the source has changed. Flag "true conflicts" where both source and destination have been modified since the last sync.

     1. Compare three versions: Transformed Source, Current Destination, and Original Snapshot.  
     2. Safe Update: If Source changed but Destination has not (Current Destination \== Original Snapshot), the update is safe and applied automatically.  
     3. True Conflict: If both Source and Destination have changed, create a conflict record.  
  3. **Conflict UI:** Present true conflicts in an Odoo UI for manual resolution, with the ability to create dynamic rules to automate future decisions.(e.g., "Always accept source value for the cost\_price field").

### **4.4. Pluggable Transformation Pipeline**

* **Goal:** Allow users to define, manage, and sequence complex transformation logic from the Odoo UI, removing the need for filesystem access.  
* **Method:**  
  1. Create an Odoo model odf.transform.script with a code field to store individual Python transformation scripts.  
  2. The odf\_ui\_builder module will allow users to create a "Transformation Pipeline" by selecting these scripts and defining their execution order.  
  3. The odoo-data-flow library must be able to receive this ordered list of scripts and execute them sequentially, passing a Polars DataFrame from one step to the next.