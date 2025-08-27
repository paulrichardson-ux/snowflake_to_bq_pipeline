# CIN7 Core BOM Converter - Setup Guide

This guide will walk you through setting up the CIN7 Core BOM Converter in Google Sheets step by step.

## 📋 Pre-Setup Checklist

Before you begin, ensure you have:
- [ ] CIN7 Core access with BOM and Inventory export capabilities
- [ ] Google account with Google Sheets access
- [ ] Latest BOM export from CIN7 Core
- [ ] Latest Inventory export from CIN7 Core

## 🚀 Step-by-Step Setup

### Step 1: Export Data from CIN7 Core

1. **Export Inventory Data:**
   - Log into CIN7 Core
   - Navigate to Inventory → Items
   - Export all items with at least these columns:
     - Item Code
     - Item Name
     - Category
   - Save as CSV

2. **Export BOM Data:**
   - Navigate to Manufacturing → Bill of Materials
   - Export all BOMs with at least these columns:
     - Parent/Finished Good
     - Component/Raw Material
     - Quantity
   - Save as CSV

### Step 2: Create Google Sheets Document

1. Go to [Google Sheets](https://sheets.google.com)
2. Click "Create" → "Blank spreadsheet"
3. Rename the document to "CIN7 BOM Converter" or similar

### Step 3: Import Your Data

1. **Create BOMs Sheet:**
   - Rename "Sheet1" to "BOMs"
   - Import your BOM export data
   - Ensure data starts from row 1 with headers

2. **Create Inventory Sheet:**
   - Add a new sheet (+ button at bottom)
   - Name it "Inventory"
   - Import your inventory export data
   - Ensure data starts from row 1 with headers

### Step 4: Install Apps Script Code

1. **Open Apps Script:**
   - In your Google Sheet, go to `Extensions` → `Apps Script`
   - You'll see a default `Code.gs` file

2. **Add Main Code:**
   - Delete all content in `Code.gs`
   - Copy the entire content from `Code.gs` in this repository
   - Paste it into the Apps Script editor
   - Click "Save" (💾 icon)

3. **Add Utility Code:**
   - Click the "+" next to "Files" to add a new file
   - Name it "Utils.gs"
   - Copy and paste the content from `Utils.gs`
   - Save the file

4. **Add Configuration Code:**
   - Add another new file named "Config.gs"
   - Copy and paste the content from `Config.gs`
   - Save the file

5. **Set Project Name:**
   - Click "Untitled project" at the top
   - Rename to "CIN7 BOM Converter"
   - Save

### Step 5: Grant Permissions

1. **Run Initial Setup:**
   - In Apps Script, select the function `initializeBOMConverter`
   - Click "Run" (▶️ button)
   - You'll be prompted to grant permissions

2. **Grant Permissions:**
   - Click "Review permissions"
   - Choose your Google account
   - Click "Advanced" if you see a warning
   - Click "Go to CIN7 BOM Converter (unsafe)"
   - Click "Allow"

### Step 6: Initialize the System

1. **Return to Google Sheets:**
   - Go back to your Google Sheets document
   - Refresh the page (F5 or Cmd+R)
   - You should see a new menu "BOM Converter"

2. **Run Initialization:**
   - Click "BOM Converter" → "Initialize BOM Converter"
   - Wait for the success message
   - You should now see new sheets: Dashboard, Materials, BOM_Import_Template

## 🔧 Configuration and Testing

### Verify Data Format

1. **Check Configuration:**
   - Go to `Extensions` → `Apps Script`
   - In the Apps Script editor, select `showConfigurationStatus`
   - Click "Run"
   - Check the execution log for any issues

2. **Test Finished Goods Dropdown:**
   - Go to the "Dashboard" sheet
   - Click on cell B3 (Finished Good dropdown)
   - Verify you see finished goods from your inventory

### Test Basic Workflow

1. **Select a Test Product:**
   - Choose a finished good from the dropdown
   - Enter a test batch yield (e.g., 10)

2. **Load Materials:**
   - Go to "BOM Converter" menu → "Load Materials for Finished Good"
   - Verify materials appear in the Materials sheet

3. **Test Calculation:**
   - Enter some test consumption values in the Materials sheet
   - Go to "BOM Converter" menu → "Calculate Per-Case Consumption"
   - Verify calculations appear correct

## 🐛 Troubleshooting Common Setup Issues

### Issue: "BOM Converter" Menu Not Appearing
**Solution:**
- Refresh the Google Sheets page
- Ensure Apps Script code is saved
- Check that `onOpen()` function exists in Code.gs

### Issue: "No finished goods found"
**Solution:**
- Verify inventory data has items with category "stock"
- Check column mapping in Config.gs
- Ensure inventory sheet is named exactly "Inventory"

### Issue: Permission Errors
**Solution:**
- Re-run the permission grant process
- Ensure you're logged into the correct Google account
- Try running `initializeBOMConverter` again in Apps Script

### Issue: "Required sheets not found"
**Solution:**
- Verify sheet names match exactly: "BOMs", "Inventory"
- Check for extra spaces or different capitalization
- Re-import data if necessary

### Issue: Column Mapping Problems
**Solution:**
- Run `showConfigurationStatus` in Apps Script
- Check the execution log for detected columns
- Manually adjust column mappings in Config.gs if needed

## 📊 Data Format Requirements

### Inventory Sheet Format
```
Row 1: Item Code | Item Name | Category | ...
Row 2: WIDGET001 | Widget A  | stock    | ...
Row 3: MAT001    | Steel     | raw      | ...
```

### BOMs Sheet Format
```
Row 1: Finished Good | Raw Material | Quantity | ...
Row 2: Widget A      | Steel        | 2.5      | ...
Row 3: Widget A      | Plastic      | 1.0      | ...
```

## ✅ Verification Checklist

After setup, verify:
- [ ] BOM Converter menu appears in Google Sheets
- [ ] Dashboard sheet shows finished goods dropdown
- [ ] Can load materials for a selected finished good
- [ ] Can calculate per-case consumption
- [ ] Can generate BOM import template
- [ ] Export template has correct CIN7 format

## 📞 Support

If you encounter issues:

1. **Check Apps Script Logs:**
   - Go to `Extensions` → `Apps Script`
   - Click "Executions" to see detailed logs

2. **Verify Data Format:**
   - Ensure your CIN7 exports match expected format
   - Check for special characters or formatting issues

3. **Test with Sample Data:**
   - Try with a small subset of data first
   - Verify basic functionality before full implementation

## 🔄 Next Steps

Once setup is complete:
1. Review the main [README.md](README.md) for usage instructions
2. Test with a small batch before processing production data
3. Set up regular data export schedule from CIN7
4. Train team members on the workflow

---

**Setup Complete!** 🎉

You're now ready to convert production batch data into CIN7 Core BOM imports. Follow the usage workflow in the main README to start processing your first batch.
