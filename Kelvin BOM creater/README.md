# CIN7 Core BOM Converter - Google Apps Script

This Google Apps Script solution helps convert production batch data into CIN7 Core BOM import format by calculating raw materials consumed per case based on actual batch consumption.

## 🎯 Purpose

The BOM Converter streamlines the process of updating Bill of Materials in CIN7 Core based on actual production batch consumption data. It calculates the precise per-case material consumption from total batch consumption, ensuring accurate BOM updates.

## 📋 Prerequisites

1. **Google Sheets** with the following tabs:
   - `BOMs` - Export from CIN7 Core containing Bill of Materials data
   - `Inventory` - Export from CIN7 Core containing inventory data

2. **CIN7 Core Data Format Requirements:**
   - **BOMs Tab**: Must contain columns for Finished Good, Raw Material Code, and Quantity
   - **Inventory Tab**: Must contain columns for Item Code, Item Name, and Category
   - **Finished Goods**: Items with category "stock" in the Inventory tab

## 🚀 Setup Instructions

### Step 1: Create Google Sheets Document
1. Create a new Google Sheets document
2. Import your CIN7 Core data:
   - Create a tab named `BOMs` and paste your BOM export data
   - Create a tab named `Inventory` and paste your inventory export data

### Step 2: Add Apps Script Code
1. In Google Sheets, go to `Extensions` → `Apps Script`
2. Delete the default `Code.gs` content
3. Copy and paste the entire contents of `Code.gs` from this repository
4. Save the project (Ctrl+S or Cmd+S)

### Step 3: Initialize the System
1. Return to your Google Sheets document
2. Refresh the page to load the custom menu
3. Go to `BOM Converter` menu → `Initialize BOM Converter`
4. Grant necessary permissions when prompted

## 📊 Usage Workflow

### Step 1: Select Finished Good
1. Go to the `Dashboard` sheet
2. Select a finished good from the dropdown (populated from inventory items with category "stock")
3. Enter the batch yield (number of cases produced)

### Step 2: Load Materials
1. Click `BOM Converter` menu → `Load Materials for Finished Good`
2. The system will populate the `Materials` sheet with all raw materials for the selected finished good

### Step 3: Enter Consumption Data
1. Go to the `Materials` sheet
2. Enter the total quantity consumed for each raw material in the "Total Consumed" column
3. Add additional materials if needed using `BOM Converter` menu → `Add Custom Material`

### Step 4: Calculate Per-Case Consumption
1. Click `BOM Converter` menu → `Calculate Per-Case Consumption`
2. The system will calculate materials consumed per case by dividing total consumption by batch yield

### Step 5: Generate Import Template
1. Click `BOM Converter` menu → `Generate BOM Import Template`
2. The `BOM_Import_Template` sheet will be populated with CIN7 Core compatible data

### Step 6: Download and Import
1. Go to the `BOM_Import_Template` sheet
2. Download as CSV: `File` → `Download` → `Comma-separated values (.csv)`
3. Upload the CSV to CIN7 Core to update your BOM

## 📑 Sheet Structure

### Dashboard Sheet
- **Finished Good**: Dropdown selection from stock items
- **Batch Yield**: Number of cases produced in the batch

### Materials Sheet
- **Raw Material Code**: Material identifier from inventory
- **Raw Material Name**: Material description
- **BOM Quantity**: Original BOM quantity per case
- **Total Consumed**: User input - total quantity used in batch
- **Per Case Calculated**: Calculated consumption per case

### BOM_Import_Template Sheet
- **Parent Item Code**: Finished good code
- **Parent Item Name**: Finished good name
- **Component Item Code**: Raw material code
- **Component Item Name**: Raw material name
- **Quantity**: Calculated per-case consumption
- **Unit**: Unit of measure (default: EA)

## 🛠️ Menu Functions

| Function | Description |
|----------|-------------|
| Initialize BOM Converter | Sets up all required sheets and structures |
| Load Materials for Finished Good | Populates materials based on selected finished good |
| Add Custom Material | Adds additional materials from inventory |
| Calculate Per-Case Consumption | Calculates per-case values from total consumption |
| Generate BOM Import Template | Creates CIN7 Core compatible import file |
| Download BOM Template Info | Shows instructions for downloading CSV |

## 🔧 Data Validation Features

- **Finished Good Validation**: Only shows items categorized as "stock"
- **Material Validation**: Custom materials must exist in inventory
- **Numeric Validation**: Ensures valid batch yield and consumption quantities
- **Error Handling**: Comprehensive error messages for common issues

## 📈 Example Workflow

1. **Import Data**: BOMs and Inventory from CIN7 Core
2. **Select Product**: "Widget A" with batch yield of 100 cases
3. **Load Materials**: System finds materials: Steel (2kg), Plastic (1kg), Screws (10 units)
4. **Enter Consumption**: Steel: 250kg, Plastic: 120kg, Screws: 1200 units
5. **Calculate**: Per case - Steel: 2.5kg, Plastic: 1.2kg, Screws: 12 units
6. **Export**: Generate CSV for CIN7 Core import

## 🚨 Important Notes

- **Data Accuracy**: Ensure your CIN7 exports are current and complete
- **Column Mapping**: Verify column positions match your CIN7 export format
- **Backup**: Always backup your existing BOMs before importing updates
- **Testing**: Test with a small batch before processing large datasets

## 🐛 Troubleshooting

### Common Issues

1. **"No finished goods found"**
   - Ensure inventory items have category "stock"
   - Check column mapping in inventory data

2. **"No materials found for finished good"**
   - Verify BOM data contains the selected finished good
   - Check spelling and exact matches

3. **Menu not appearing**
   - Refresh the Google Sheets page
   - Ensure Apps Script code is saved properly

### Support

For technical issues:
1. Check the Apps Script execution log: `Extensions` → `Apps Script` → `Executions`
2. Verify data format matches expected structure
3. Ensure all required sheets exist and contain data

## 🔄 Version History

- **v1.0**: Initial release with core BOM conversion functionality
- Features: Dashboard setup, material loading, consumption calculation, CIN7 export

---

*This tool streamlines BOM management by bridging actual production data with CIN7 Core requirements, ensuring accurate material planning and cost tracking.*
