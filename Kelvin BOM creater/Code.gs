/**
 * CIN7 Core BOM Converter for Google Sheets
 * 
 * This script helps convert production batch data into CIN7 Core BOM import format
 * by calculating raw materials consumed per case based on actual batch consumption.
 */

/**
 * Helper function to setup dropdown for a given dashboard sheet
 */
function setupDropdownForDashboard(dashboardSheet) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const inventorySheet = ss.getSheetByName('Inventory');
  
  if (!inventorySheet) {
    throw new Error('Inventory sheet not found');
  }
  
  // Get all inventory data
  const inventoryData = inventorySheet.getDataRange().getValues();
  
  if (inventoryData.length < 2) {
    throw new Error('No data found in Inventory sheet');
  }
  
  // Find the column indices based on your sheet structure
  let nameColumn = 1;     // Column B (Name)
  let categoryColumn = 2; // Column C (Category)
  let brandColumn = 3;    // Column D (Brand)
  let typeColumn = 4;     // Column E (Type)
  
  // Filter for Slush Mix products (check multiple columns)
  const slushMixProducts = [];
  
  for (let i = 1; i < inventoryData.length; i++) {
    const row = inventoryData[i];
    const name = row[nameColumn];
    const category = row[categoryColumn] ? row[categoryColumn].toString().toLowerCase().trim() : '';
    const brand = row[brandColumn] ? row[brandColumn].toString().toLowerCase().trim() : '';
    const type = row[typeColumn] ? row[typeColumn].toString().toLowerCase().trim() : '';
    
    if (name) {
      // Look for "Slush Mix" in category, brand, type, or name
      if (category.includes('slush mix') || category.includes('slush') ||
          brand.includes('slush mix') || brand.includes('slush') ||
          type.includes('slush mix') || type.includes('slush') ||
          name.toString().toLowerCase().includes('slush')) {
        slushMixProducts.push(name.toString());
      }
    }
  }
  
  if (slushMixProducts.length === 0) {
    throw new Error('No Slush Mix products found');
  }
  
  // Remove duplicates and sort
  const uniqueSlushMixProducts = [...new Set(slushMixProducts)].sort();
  
  // Create dropdown validation rule
  const rule = SpreadsheetApp.newDataValidation()
    .requireValueInList(uniqueSlushMixProducts)
    .setAllowInvalid(false)
    .setHelpText('Select a Slush Mix product from your inventory')
    .build();
  
  // Apply to the finished good cell (B5:C5 merged)
  dashboardSheet.getRange('B5:C5').setDataValidation(rule);
  
  return uniqueSlushMixProducts.length;
}

/**
 * Setup dropdown for finished goods in Dashboard sheet
 */
function setupFinishedGoodDropdownInDashboard() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const dashboardSheet = ss.getSheetByName('Dashboard');
  const inventorySheet = ss.getSheetByName('Inventory');
  
  if (!dashboardSheet) {
    SpreadsheetApp.getUi().alert('Dashboard sheet not found. Please ensure you have a "Dashboard" sheet.');
    return;
  }
  
  if (!inventorySheet) {
    SpreadsheetApp.getUi().alert('Inventory sheet not found. Please ensure you have an "Inventory" sheet with your product data.');
    return;
  }
  
  try {
    const productCount = setupDropdownForDashboard(dashboardSheet);
    SpreadsheetApp.getUi().alert(`Dropdown created with ${productCount} Slush Mix products!`);
    
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error setting up dropdown: ' + error.toString());
    Logger.log('Dropdown setup error:', error);
  }
}

/**
 * Load materials based on selected finished good using Product SKU lookup
 */
function loadMaterialsForFinishedGood() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const dashboardSheet = ss.getSheetByName('Dashboard');
  const bomsSheet = ss.getSheetByName('BOMs');
  const inventorySheet = ss.getSheetByName('Inventory');
  
  // Get selected finished good
  const finishedGood = dashboardSheet.getRange('B5').getValue();
  
  if (!finishedGood) {
    SpreadsheetApp.getUi().alert('Please select a finished good first.');
    return;
  }
  
  if (!bomsSheet) {
    SpreadsheetApp.getUi().alert('Please ensure the "BOMs" sheet exists with your CIN7 BOM data.');
    return;
  }
  
  if (!inventorySheet) {
    SpreadsheetApp.getUi().alert('Please ensure the "Inventory" sheet exists.');
    return;
  }
  
  try {
    // Get data from sheets
    const bomData = bomsSheet.getDataRange().getValues();
    const inventoryData = inventorySheet.getDataRange().getValues();
    
    // Create inventory lookup for SKU to name mapping and units
    const inventoryLookup = {};
    const nameToSKULookup = {};
    
    inventoryData.forEach((row, index) => {
      if (index > 0 && row[0]) { // ProductCode in column A
        const sku = row[0];
        const name = row[1] || '';
        const unit = row[43] || 'EA'; // Unit in column AR (0-based index 43)
        
        inventoryLookup[sku] = {
          name: name,
          unit: unit
        };
        
        // Also create reverse lookup from name to SKU
        if (name) {
          nameToSKULookup[name.toString().trim()] = sku;
        }
      }
    });
    
    // Get the SKU for the selected finished good
    const finishedGoodSKU = nameToSKULookup[finishedGood.toString().trim()];
    
    if (!finishedGoodSKU) {
      SpreadsheetApp.getUi().alert(`Could not find SKU for "${finishedGood}" in inventory. Please ensure the product exists in the Inventory sheet.`);
      return;
    }
    
    Logger.log('Looking for finished good SKU:', finishedGoodSKU);
    Logger.log('Finished good name:', finishedGood);
    
    // Filter materials for selected finished good using SKU
    // BOM structure: Column B = ProductSKU, Column C = ProductName, Column D = ComponentSKU, Column E = ComponentName, Column F = Quantity
    const materials = [];
    let foundMatches = 0;
    
    for (let i = 1; i < bomData.length; i++) {
      const row = bomData[i];
      const productSKU = row[1]; // Column B (ProductSKU)  
      const productName = row[2]; // Column C (ProductName)
      const componentSKU = row[3]; // Column D (ComponentSKU)
      const componentName = row[4]; // Column E (ComponentName)
      const quantity = row[5]; // Column F (Quantity)
      
      // Match by SKU - much more reliable
      if (productSKU && productSKU.toString().trim() === finishedGoodSKU.toString().trim()) {
        foundMatches++;
        const inventoryInfo = inventoryLookup[componentSKU] || {};
        const unit = inventoryInfo.unit || 'EA';
        
        materials.push([
          componentSKU || '', // Component SKU
          componentName || inventoryInfo.name || '', // Component Name
          '', // Total Consumed (to be filled by user)
          '', // Per Case Calculated
          unit // Unit from inventory
        ]);
        
        Logger.log(`Match found for SKU ${productSKU}: component ${componentName} (${componentSKU})`);
      }
    }
    
    Logger.log(`Found ${foundMatches} material entries for SKU ${finishedGoodSKU}`);
    
    // If no matches with ProductSKU in column B, try ProductName in column C as fallback
    if (materials.length === 0) {
      Logger.log('No SKU matches found, trying name-based matching as fallback...');
      
      for (let i = 1; i < bomData.length; i++) {
        const row = bomData[i];
        const productName = row[2]; // Column C (ProductName)
        const componentSKU = row[3]; // Column D (ComponentSKU)
        const componentName = row[4]; // Column E (ComponentName)
        const quantity = row[5]; // Column F (Quantity)
        
        if (productName && productName.toString().trim() === finishedGood.toString().trim()) {
          const inventoryInfo = inventoryLookup[componentSKU] || {};
          const unit = inventoryInfo.unit || 'EA';
          
          materials.push([
            componentSKU || '',
            componentName || inventoryInfo.name || '',
            '',
            '',
            unit
          ]);
          
          Logger.log(`Fallback match found: "${productName}" with component: ${componentName}`);
        }
      }
    }
    
    if (materials.length === 0) {
      SpreadsheetApp.getUi().alert(`No materials found for "${finishedGood}" in the BOMs sheet. Please check the product name matches exactly.`);
      return;
    }
    
    // Create or get materials sheet
    let materialsSheet = ss.getSheetByName('Materials');
    if (!materialsSheet) {
      materialsSheet = ss.insertSheet('Materials');
    }
    
    // Clear existing borders and content
    materialsSheet.getRange('A1:Z100').setBorder(false, false, false, false, false, false);
    
    // Set up professional headers with proper borders
    const headers = ['Component SKU', 'Component Name', 'Total Consumed', 'Per Case Calculated', 'Unit'];
    materialsSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    materialsSheet.getRange(1, 1, 1, headers.length)
      .setFontWeight('bold')
      .setBackground('#4285f4')
      .setFontColor('white')
      .setVerticalAlignment('middle')
      .setHorizontalAlignment('center')
      .setBorder(true, true, true, true, true, true, '#2c5aa0', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
    
    // Set column widths for better readability
    materialsSheet.setColumnWidth(1, 150); // Component SKU
    materialsSheet.setColumnWidth(2, 280); // Component Name (wider now)
    materialsSheet.setColumnWidth(3, 180); // Total Consumed
    materialsSheet.setColumnWidth(4, 180); // Per Case Calculated
    materialsSheet.setColumnWidth(5, 80);  // Unit
    
    // Clear existing materials and add new ones
    if (materialsSheet.getLastRow() > 1) {
      materialsSheet.getRange(2, 1, materialsSheet.getLastRow(), 5).clearContent();
    }
    materialsSheet.getRange(2, 1, materials.length, 5).setValues(materials);
    
    // Apply professional styling to data rows
    if (materials.length > 0) {
      // Add borders and formatting to the entire data area
      const dataRange = materialsSheet.getRange(2, 1, materials.length, 5);
      
      // Set consistent formatting for all data cells
      dataRange
        .setVerticalAlignment('middle')
        .setHorizontalAlignment('left')
        .setBorder(true, true, true, true, true, true, '#d0d0d0', SpreadsheetApp.BorderStyle.SOLID);
      
      // Apply alternating row colors for better readability
      for (let i = 0; i < materials.length; i++) {
        const row = i + 2;
        const backgroundColor = i % 2 === 0 ? '#ffffff' : '#f8f9fa';
        materialsSheet.getRange(row, 1, 1, 5).setBackground(backgroundColor);
      }
      
      // Highlight input column (Total Consumed) with special formatting
      materialsSheet.getRange(2, 3, materials.length, 1)
        .setBackground('#fff2cc')
        .setFontWeight('normal')
        .setBorder(true, true, true, true, true, true, '#ffa000', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
      
      // Format calculated column (Per Case) with subtle highlight
      materialsSheet.getRange(2, 4, materials.length, 1)
        .setBackground('#e8f5e8')
        .setFontStyle('italic');
    }
    
    // Set row height for better spacing
    materialsSheet.setRowHeight(1, 35); // Header row
    for (let i = 2; i <= materials.length + 1; i++) {
      materialsSheet.setRowHeight(i, 28);
    }
    
    SpreadsheetApp.getUi().alert(`Loaded ${materials.length} materials for "${finishedGood}"`);
    
  } catch (error) {
    SpreadsheetApp.getUi().alert('Error loading materials: ' + error.toString());
    Logger.log('Load materials error:', error);
  }
}

/**
 * Calculate per-case consumption for current sheet structure
 */
function calculatePerCaseFromCurrentSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const materialsSheet = ss.getSheetByName('Materials');
  const dashboardSheet = ss.getSheetByName('Dashboard');
  
  if (!materialsSheet) {
    SpreadsheetApp.getUi().alert('Materials sheet not found. Please load materials first.');
    return;
  }
  
  // Get batch yield from Dashboard B6
  let batchYield;
  if (dashboardSheet) {
    batchYield = dashboardSheet.getRange('B6').getValue();
  }
  
  // If not found in dashboard, try current sheet B6
  if (!batchYield || batchYield <= 0) {
    const currentSheet = ss.getActiveSheet();
    batchYield = currentSheet.getRange('B6').getValue();
  }
  
  if (!batchYield || batchYield <= 0) {
    SpreadsheetApp.getUi().alert('Please enter a valid batch yield in Dashboard sheet cell B6.');
    return;
  }
  
  // Find the last row with data in Materials sheet
  const lastRow = materialsSheet.getLastRow();
  
  if (lastRow < 2) {
    SpreadsheetApp.getUi().alert('No materials found. Please load materials first.');
    return;
  }
  
  // Calculate per-case consumption for each material
  for (let row = 2; row <= lastRow; row++) {
    const totalConsumed = materialsSheet.getRange(row, 3).getValue(); // Column C (Total Consumed)
    
    if (totalConsumed && !isNaN(totalConsumed) && totalConsumed > 0) {
      const perCase = totalConsumed / batchYield;
      materialsSheet.getRange(row, 4).setValue(perCase.toFixed(6)); // Column D (Per Case Calculated)
    }
  }
  
  // Format per-case column
  materialsSheet.getRange(2, 4, lastRow - 1, 1).setBackground('#d9ead3');
  
  SpreadsheetApp.getUi().alert(`Per-case consumption calculated for batch yield of ${batchYield} cases!`);
}

/**
 * Generate CIN7 BOM import from Materials sheet
 */
function generateBOMFromCurrentSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const materialsSheet = ss.getSheetByName('Materials');
  const dashboardSheet = ss.getSheetByName('Dashboard');
  const inventorySheet = ss.getSheetByName('Inventory');
  
  if (!materialsSheet) {
    SpreadsheetApp.getUi().alert('Materials sheet not found. Please load materials first.');
    return;
  }
  
  // Get finished good name from Dashboard
  let finishedGood;
  if (dashboardSheet) {
    finishedGood = dashboardSheet.getRange('B5').getValue();
  }
  
  if (!finishedGood) {
    SpreadsheetApp.getUi().alert('Please select a finished good in Dashboard sheet cell B5.');
    return;
  }
  
  // Get finished good SKU from inventory
  let finishedGoodSKU = finishedGood; // Default to name if SKU not found
  
  if (inventorySheet) {
    const inventoryData = inventorySheet.getDataRange().getValues();
    for (let i = 1; i < inventoryData.length; i++) {
      const name = inventoryData[i][1]; // Column B (Name)
      const sku = inventoryData[i][0];  // Column A (SKU)
      
      if (name && name.toString().trim() === finishedGood.toString().trim()) {
        finishedGoodSKU = sku;
        break;
      }
    }
  }
  
  // Create or get export sheet
  let exportSheet = ss.getSheetByName('BOM_Export');
  if (!exportSheet) {
    exportSheet = ss.insertSheet('BOM_Export');
  }
  
  // Clear and setup headers
  exportSheet.clear();
  const headers = ['Parent Item Code', 'Parent Item Name', 'Component Item Code', 'Component Item Name', 'Quantity', 'Unit'];
  exportSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  exportSheet.getRange(1, 1, 1, headers.length).setFontWeight('bold').setBackground('#34a853').setFontColor('white');
  
  // Get material data and create export rows
  const lastRow = materialsSheet.getLastRow();
  const exportData = [];
  
  for (let row = 2; row <= lastRow; row++) {
    const componentSKU = materialsSheet.getRange(row, 1).getValue();   // Column A
    const componentName = materialsSheet.getRange(row, 2).getValue();  // Column B
    const perCaseQty = materialsSheet.getRange(row, 4).getValue();     // Column D
    const unit = materialsSheet.getRange(row, 5).getValue() || 'EA';   // Column E
    
    if (componentName && perCaseQty && perCaseQty > 0) {
      exportData.push([
        finishedGoodSKU,       // Parent Item Code
        finishedGood,          // Parent Item Name
        componentSKU || componentName, // Component Item Code
        componentName,         // Component Item Name
        parseFloat(perCaseQty), // Quantity
        unit                   // Unit
      ]);
    }
  }
  
  if (exportData.length === 0) {
    SpreadsheetApp.getUi().alert('No materials with per-case quantities found. Please calculate per-case consumption first.');
    return;
  }
  
  // Write export data
  exportSheet.getRange(2, 1, exportData.length, 6).setValues(exportData);
  
  // Format the sheet
  exportSheet.autoResizeColumns(1, 6);
  exportSheet.getRange(2, 1, exportData.length, 6).setBorder(true, true, true, true, true, true);
  
  SpreadsheetApp.getUi().alert(`BOM export generated with ${exportData.length} components in 'BOM_Export' sheet!`);
}

/**
 * Add custom material to the materials list
 */
function addCustomMaterial() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const materialsSheet = ss.getSheetByName('Materials');
  const inventorySheet = ss.getSheetByName('Inventory');
  
  if (!materialsSheet) {
    SpreadsheetApp.getUi().alert('Materials sheet not found. Please load materials first.');
    return;
  }
  
  if (!inventorySheet) {
    SpreadsheetApp.getUi().alert('Inventory sheet not found.');
    return;
  }
  
  // Get user input for material code
  const ui = SpreadsheetApp.getUi();
  const result = ui.prompt('Add Custom Material', 'Enter the material SKU from inventory:', ui.ButtonSet.OK_CANCEL);
  
  if (result.getSelectedButton() === ui.Button.CANCEL) {
    return;
  }
  
  const materialSKU = result.getResponseText().trim();
  if (!materialSKU) {
    ui.alert('Please enter a valid material SKU.');
    return;
  }
  
  // Validate material exists in inventory
  const inventoryData = inventorySheet.getDataRange().getValues();
  let materialName = '';
  let unit = 'EA';
  let found = false;
  
  for (let i = 1; i < inventoryData.length; i++) {
    if (inventoryData[i][0] === materialSKU) { // Column A (SKU)
      materialName = inventoryData[i][1] || ''; // Column B (Name)
      unit = inventoryData[i][43] || 'EA'; // Column AR (Unit)
      found = true;
      break;
    }
  }
  
  if (!found) {
    ui.alert('Material SKU not found in inventory sheet.');
    return;
  }
  
  // Add to materials sheet
  const lastRow = materialsSheet.getLastRow();
  const newRow = [materialSKU, materialName, '', '', unit];
  
  materialsSheet.getRange(lastRow + 1, 1, 1, 5).setValues([newRow]);
  materialsSheet.getRange(lastRow + 1, 3).setBackground('#fff2cc'); // Highlight consumed column
  
  ui.alert(`Added custom material: ${materialSKU} - ${materialName}`);
}

/**
 * Debug function to show all unique product names in BOM sheet
 */
function debugShowBOMProducts() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const bomsSheet = ss.getSheetByName('BOMs');
  
  if (!bomsSheet) {
    SpreadsheetApp.getUi().alert('BOMs sheet not found');
    return;
  }
  
  const bomData = bomsSheet.getDataRange().getValues();
  const uniqueProducts = new Set();
  
  for (let i = 1; i < bomData.length; i++) {
    const productSKU = bomData[i][1]; // Column B (ProductSKU)
    const productName = bomData[i][2]; // Column C (ProductName)
    if (productName) {
      uniqueProducts.add(`${productSKU} - ${productName.toString().trim()}`);
    }
  }
  
  const productList = Array.from(uniqueProducts).sort().join('\n');
  SpreadsheetApp.getUi().alert('Products in BOM Sheet', productList, SpreadsheetApp.getUi().ButtonSet.OK);
}

/**
 * Initialize BOM Converter with default tabs and formatting
 */
function initializeBOMConverter() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  try {
    // Create Dashboard tab
    createDashboardTab(ss);
    
    // Create Materials tab (template)
    createMaterialsTab(ss);
    
    // Create Instructions tab
    createInstructionsTab(ss);
    
    SpreadsheetApp.getUi().alert('BOM Converter Setup Complete!', 
      'Default tabs have been created:\n\n' +
      '• Dashboard - Select products and enter batch details\n' +
      '• Materials - View and edit material consumption\n' +
      '• Instructions - Step-by-step guide\n\n' +
      'Import your BOMs and Inventory data, then start with the Dashboard!', 
      SpreadsheetApp.getUi().ButtonSet.OK);
    
  } catch (error) {
    SpreadsheetApp.getUi().alert('Setup Error', 'Error creating tabs: ' + error.toString(), SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

/**
 * Create and format the Dashboard tab
 */
function createDashboardTab(ss) {
  let dashboardSheet = ss.getSheetByName('Dashboard');
  if (!dashboardSheet) {
    dashboardSheet = ss.insertSheet('Dashboard');
  }
  
  // Clear existing content
  dashboardSheet.clear();
  
  // Clear all existing borders first
  dashboardSheet.getRange('A1:Z50').setBorder(false, false, false, false, false, false);
  
  // Set up the dashboard layout with rounded corners effect
  dashboardSheet.getRange('A1').setValue('🏭 CIN7 BOM Converter Dashboard').setFontSize(18).setFontWeight('bold');
  dashboardSheet.getRange('A1:G1').merge()
    .setBackground('#4285f4')
    .setFontColor('white')
    .setHorizontalAlignment('center')
    .setVerticalAlignment('middle');
  
  // Production Batch Section with subtle styling
  dashboardSheet.getRange('A3:G3').merge()
    .setValue('📦 Production Batch Details')
    .setFontSize(14)
    .setFontWeight('bold')
    .setBackground('#e8f0fe')
    .setHorizontalAlignment('left')
    .setVerticalAlignment('middle');
  
  // Labels with clean styling
  dashboardSheet.getRange('A5').setValue('Finished Good:').setFontWeight('bold').setVerticalAlignment('middle');
  dashboardSheet.getRange('A6').setValue('Batch Yield (Cases):').setFontWeight('bold').setVerticalAlignment('middle');
  dashboardSheet.getRange('A7').setValue('Production Date:').setFontWeight('bold').setVerticalAlignment('middle');
  
  // Input cells with modern styling - no heavy borders, just subtle background
  dashboardSheet.getRange('B5:C5').merge()
    .setBackground('#fff2cc')
    .setVerticalAlignment('middle')
    .setBorder(true, true, true, true, false, false, '#d4d4d4', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
    
  dashboardSheet.getRange('B6:C6').merge()
    .setBackground('#fff2cc')
    .setVerticalAlignment('middle')
    .setBorder(true, true, true, true, false, false, '#d4d4d4', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
    
  dashboardSheet.getRange('B7:C7').merge()
    .setBackground('#fff2cc')
    .setVerticalAlignment('middle')
    .setBorder(true, true, true, true, false, false, '#d4d4d4', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
  
  // Try to setup dropdown immediately if Inventory sheet exists
  try {
    setupDropdownForDashboard(dashboardSheet);
  } catch (e) {
    // If setup fails, user will need to run it manually
    Logger.log('Could not auto-setup dropdown:', e);
  }
  
  // Add instructions with clean styling
  dashboardSheet.getRange('D5:G5').merge()
    .setValue('← Select from dropdown (Auto-setup if Inventory exists)')
    .setFontStyle('italic')
    .setFontColor('#666666')
    .setVerticalAlignment('middle')
    .setFontSize(9);
    
  dashboardSheet.getRange('D6:G6').merge()
    .setValue('← Enter number of cases produced')
    .setFontStyle('italic')
    .setFontColor('#666666')
    .setVerticalAlignment('middle')
    .setFontSize(9);
    
  dashboardSheet.getRange('D7:G7').merge()
    .setValue('← Optional: Enter production date')
    .setFontStyle('italic')
    .setFontColor('#666666')
    .setVerticalAlignment('middle')
    .setFontSize(9);
  
  // Workflow Section with card-like appearance
  dashboardSheet.getRange('A9:G9').merge()
    .setValue('⚡ Workflow Steps')
    .setFontSize(14)
    .setFontWeight('bold')
    .setBackground('#e8f0fe')
    .setHorizontalAlignment('left')
    .setVerticalAlignment('middle');
  
  const steps = [
    ['1.', 'Setup Finished Good Dropdown', '(BOM Converter → Setup Finished Good Dropdown)'],
    ['2.', 'Select Finished Good', '(Choose from dropdown above)'],
    ['3.', 'Load Materials', '(BOM Converter → Load Materials for Finished Good)'],
    ['4.', 'Enter Consumption Data', '(Go to Materials tab, fill Total Consumed column)'],
    ['5.', 'Calculate Per-Case', '(BOM Converter → Calculate Per-Case Consumption)'],
    ['6.', 'Generate Export', '(BOM Converter → Generate CIN7 BOM Export)']
  ];
  
  // Create workflow steps with modern card-like design
  for (let i = 0; i < steps.length; i++) {
    const row = 11 + i;
    
    // Step number with circular background
    dashboardSheet.getRange(row, 1)
      .setValue(steps[i][0])
      .setFontWeight('bold')
      .setBackground('#4285f4')
      .setFontColor('white')
      .setHorizontalAlignment('center')
      .setVerticalAlignment('middle')
      .setBorder(true, true, true, true, false, false, '#4285f4', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
    
    // Step name
    dashboardSheet.getRange(row, 2)
      .setValue(steps[i][1])
      .setFontWeight('bold')
      .setVerticalAlignment('middle')
      .setBackground('#f8f9fa');
    
    // Instructions with subtle styling
    dashboardSheet.getRange(row, 3, 1, 5).merge()
      .setValue(steps[i][2])
      .setFontStyle('italic')
      .setFontColor('#666666')
      .setVerticalAlignment('middle')
      .setFontSize(9)
      .setBackground('#f8f9fa');
  }
  
  // Create a subtle border around the entire workflow section
  dashboardSheet.getRange('A11:G16')
    .setBorder(true, true, true, true, false, true, '#e0e0e0', SpreadsheetApp.BorderStyle.SOLID);
  
  // Set column widths for better alignment
  dashboardSheet.setColumnWidth(1, 40);   // Step numbers
  dashboardSheet.setColumnWidth(2, 220);  // Step names
  dashboardSheet.setColumnWidth(3, 100);  // Instructions start
  dashboardSheet.setColumnWidth(4, 100);
  dashboardSheet.setColumnWidth(5, 100);
  dashboardSheet.setColumnWidth(6, 100);
  dashboardSheet.setColumnWidth(7, 100);  // Instructions end
  
  // Set row heights for better spacing
  for (let i = 5; i <= 7; i++) {
    dashboardSheet.setRowHeight(i, 35);
  }
  for (let i = 11; i <= 16; i++) {
    dashboardSheet.setRowHeight(i, 28);
  }
  
  // Add some spacing
  dashboardSheet.setRowHeight(2, 10);
  dashboardSheet.setRowHeight(4, 10);
  dashboardSheet.setRowHeight(8, 10);
  dashboardSheet.setRowHeight(10, 10);
}

/**
 * Create and format the Materials tab template
 */
function createMaterialsTab(ss) {
  let materialsSheet = ss.getSheetByName('Materials');
  if (!materialsSheet) {
    materialsSheet = ss.insertSheet('Materials');
  }
  
  // Clear existing content and borders
  materialsSheet.clear();
  materialsSheet.getRange('A1:Z100').setBorder(false, false, false, false, false, false);
  
  // Set up professional headers with proper borders
  const headers = ['Component SKU', 'Component Name', 'Total Consumed', 'Per Case Calculated', 'Unit'];
  materialsSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  materialsSheet.getRange(1, 1, 1, headers.length)
    .setFontWeight('bold')
    .setBackground('#4285f4')
    .setFontColor('white')
    .setVerticalAlignment('middle')
    .setHorizontalAlignment('center')
    .setBorder(true, true, true, true, true, true, '#2c5aa0', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
  
  // Add sample/template row with professional styling
  const sampleRow = ['SAMPLE001', 'Sample Component', '← Enter actual consumption here', '← Calculated automatically', 'KG'];
  materialsSheet.getRange(2, 1, 1, headers.length).setValues([sampleRow]);
  materialsSheet.getRange(2, 1, 1, headers.length)
    .setFontStyle('italic')
    .setFontColor('#999999')
    .setVerticalAlignment('middle')
    .setHorizontalAlignment('left')
    .setBackground('#fafafa')
    .setBorder(true, true, true, true, true, true, '#d0d0d0', SpreadsheetApp.BorderStyle.SOLID);
  
  // Format input column with special highlight
  materialsSheet.getRange(2, 3)
    .setBackground('#fff2cc')
    .setBorder(true, true, true, true, true, true, '#ffa000', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
  
  // Set column widths
  materialsSheet.setColumnWidth(1, 150); // Component SKU
  materialsSheet.setColumnWidth(2, 280); // Component Name (wider now)
  materialsSheet.setColumnWidth(3, 180); // Total Consumed
  materialsSheet.setColumnWidth(4, 180); // Per Case Calculated
  materialsSheet.setColumnWidth(5, 80);  // Unit
  
  // Add instructions
  materialsSheet.getRange('A4').setValue('📋 Instructions:').setFontWeight('bold').setFontSize(12);
  materialsSheet.getRange('A5').setValue('1. Load materials using BOM Converter → Load Materials for Finished Good');
  materialsSheet.getRange('A6').setValue('2. Enter actual consumption quantities in the "Total Consumed" column (yellow)');
  materialsSheet.getRange('A7').setValue('3. Calculate per-case using BOM Converter → Calculate Per-Case Consumption');
  materialsSheet.getRange('A8').setValue('4. Generate export using BOM Converter → Generate CIN7 BOM Export');
  
  materialsSheet.getRange('A5:A8').setFontStyle('italic').setFontColor('#666666');
}

/**
 * Create and format the Instructions tab
 */
function createInstructionsTab(ss) {
  let instructionsSheet = ss.getSheetByName('Instructions');
  if (!instructionsSheet) {
    instructionsSheet = ss.insertSheet('Instructions');
  }
  
  // Clear existing content
  instructionsSheet.clear();
  
  // Title
  instructionsSheet.getRange('A1').setValue('📖 CIN7 BOM Converter - Complete Guide').setFontSize(18).setFontWeight('bold');
  instructionsSheet.getRange('A1').setBackground('#4285f4').setFontColor('white');
  instructionsSheet.getRange('A1:E1').merge().setHorizontalAlignment('center');
  
  // Prerequisites
  instructionsSheet.getRange('A3').setValue('📋 Prerequisites').setFontSize(14).setFontWeight('bold').setBackground('#e8f0fe');
  instructionsSheet.getRange('A3:E3').merge();
  
  const prerequisites = [
    'BOMs sheet - Export your Bill of Materials from CIN7 Core',
    'Inventory sheet - Export your inventory data from CIN7 Core',
    'Make sure your inventory has items with "slush" in name/brand/category/type'
  ];
  
  for (let i = 0; i < prerequisites.length; i++) {
    instructionsSheet.getRange(5 + i, 1).setValue(`• ${prerequisites[i]}`);
  }
  
  // Step-by-step guide
  instructionsSheet.getRange('A9').setValue('⚡ Step-by-Step Workflow').setFontSize(14).setFontWeight('bold').setBackground('#e8f0fe');
  instructionsSheet.getRange('A9:E9').merge();
  
  const steps = [
    ['Step 1: Setup Dropdown', 'Go to Dashboard → BOM Converter menu → "Setup Finished Good Dropdown"', 'Creates dropdown with all slush products'],
    ['Step 2: Select Product', 'In Dashboard, select your finished good from the dropdown in cell B5', 'Choose the product you produced'],
    ['Step 3: Enter Batch Details', 'Enter batch yield (cases) in cell B6, optionally add production date', 'How many cases were produced'],
    ['Step 4: Load Materials', 'BOM Converter menu → "Load Materials for Finished Good"', 'Loads all components from BOM data'],
    ['Step 5: Enter Consumption', 'Go to Materials tab, enter actual quantities used in "Total Consumed" column', 'Yellow highlighted column for input'],
    ['Step 6: Calculate Per-Case', 'BOM Converter menu → "Calculate Per-Case Consumption"', 'Divides total by batch yield'],
    ['Step 7: Generate Export', 'BOM Converter menu → "Generate CIN7 BOM Export"', 'Creates CIN7-compatible import file'],
    ['Step 8: Download & Import', 'Go to BOM_Export tab, download as CSV, upload to CIN7 Core', 'Updates your BOM with actual consumption']
  ];
  
  let currentRow = 11;
  for (let i = 0; i < steps.length; i++) {
    instructionsSheet.getRange(currentRow, 1).setValue(steps[i][0]).setFontWeight('bold').setBackground('#f0f0f0');
    instructionsSheet.getRange(currentRow, 2).setValue(steps[i][1]).setFontWeight('bold');
    instructionsSheet.getRange(currentRow + 1, 2).setValue(steps[i][2]).setFontStyle('italic').setFontColor('#666666');
    currentRow += 3;
  }
  
  // Troubleshooting
  instructionsSheet.getRange(`A${currentRow + 1}`).setValue('🔧 Troubleshooting').setFontSize(14).setFontWeight('bold').setBackground('#e8f0fe');
  instructionsSheet.getRange(`A${currentRow + 1}:E${currentRow + 1}`).merge();
  
  const troubleshooting = [
    'No products in dropdown: Check that inventory has items with "slush" in name/brand/category',
    'No materials found: Verify product name matches exactly between inventory and BOM sheets',
    'Missing units: Check column AR in inventory sheet contains unit information',
    'Calculation errors: Ensure batch yield is a positive number and consumption values are numeric'
  ];
  
  currentRow += 3;
  for (let i = 0; i < troubleshooting.length; i++) {
    instructionsSheet.getRange(currentRow + i, 1).setValue(`• ${troubleshooting[i]}`);
  }
  
  // Set column widths
  instructionsSheet.setColumnWidth(1, 150);
  instructionsSheet.setColumnWidth(2, 400);
  instructionsSheet.setColumnWidth(3, 200);
  instructionsSheet.setColumnWidth(4, 150);
  instructionsSheet.setColumnWidth(5, 150);
  
  // Add borders to step sections
  instructionsSheet.getRange('A11:B35').setBorder(true, true, true, true, true, true);
}

/**
 * Setup import template sheets with proper formatting
 */
function setupImportTemplates() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  try {
    // Setup BOMs import template
    setupBOMsImportTemplate(ss);
    
    // Setup Inventory import template  
    setupInventoryImportTemplate(ss);
    
    SpreadsheetApp.getUi().alert('Import Templates Created!', 
      'Ready-to-use import templates have been created:\n\n' +
      '• BOMs sheet - Paste your CIN7 BOM export here\n' +
      '• Inventory sheet - Paste your CIN7 inventory export here\n\n' +
      'Simply copy and paste your CSV data into these sheets!', 
      SpreadsheetApp.getUi().ButtonSet.OK);
      
  } catch (error) {
    SpreadsheetApp.getUi().alert('Setup Error', 'Error creating import templates: ' + error.toString());
  }
}

/**
 * Setup BOMs import template sheet
 */
function setupBOMsImportTemplate(ss) {
  let bomsSheet = ss.getSheetByName('BOMs');
  if (!bomsSheet) {
    bomsSheet = ss.insertSheet('BOMs');
  }
  
  // Clear existing content
  bomsSheet.clear();
  bomsSheet.getRange('A1:Z100').setBorder(false, false, false, false, false, false);
  
  // Setup headers for CIN7 BOM export format
  const bomHeaders = ['Assembly Code', 'ProductSKU', 'ProductName', 'ComponentSKU', 'ComponentName', 'Quantity', 'Unit'];
  bomsSheet.getRange(1, 1, 1, bomHeaders.length).setValues([bomHeaders]);
  bomsSheet.getRange(1, 1, 1, bomHeaders.length)
    .setFontWeight('bold')
    .setBackground('#34a853')
    .setFontColor('white')
    .setVerticalAlignment('middle')
    .setHorizontalAlignment('center')
    .setBorder(true, true, true, true, true, true, '#1e7e34', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
  
  // Add instruction row
  const instructionRow = ['Paste your CIN7 BOM export data below this row →', '', '', '', '', '', ''];
  bomsSheet.getRange(2, 1, 1, bomHeaders.length).setValues([instructionRow]);
  bomsSheet.getRange(2, 1, 1, bomHeaders.length)
    .setFontStyle('italic')
    .setFontColor('#666666')
    .setBackground('#f0f8f0')
    .setVerticalAlignment('middle')
    .setBorder(true, true, true, true, true, true, '#d4edda', SpreadsheetApp.BorderStyle.SOLID);
  
  // Set column widths
  bomsSheet.setColumnWidth(1, 120); // Assembly Code
  bomsSheet.setColumnWidth(2, 120); // ProductSKU
  bomsSheet.setColumnWidth(3, 250); // ProductName
  bomsSheet.setColumnWidth(4, 120); // ComponentSKU
  bomsSheet.setColumnWidth(5, 200); // ComponentName
  bomsSheet.setColumnWidth(6, 100); // Quantity
  bomsSheet.setColumnWidth(7, 80);  // Unit
  
  // Set row heights
  bomsSheet.setRowHeight(1, 35);
  bomsSheet.setRowHeight(2, 30);
}

/**
 * Setup Inventory import template sheet
 */
function setupInventoryImportTemplate(ss) {
  let inventorySheet = ss.getSheetByName('Inventory');
  if (!inventorySheet) {
    inventorySheet = ss.insertSheet('Inventory');
  }
  
  // Clear existing content
  inventorySheet.clear();
  inventorySheet.getRange('A1:Z100').setBorder(false, false, false, false, false, false);
  
  // Setup headers for CIN7 inventory export format (first few columns)
  const inventoryHeaders = ['ProductCode', 'Name', 'Category', 'Brand', 'Type', 'Description', 'StockLevel', 'UnitCost'];
  
  // Add more columns up to AR (column 44) for the unit field
  const allHeaders = [...inventoryHeaders];
  for (let i = inventoryHeaders.length; i < 44; i++) {
    allHeaders.push(`Column${String.fromCharCode(65 + i)}`);
  }
  allHeaders[43] = 'Unit'; // Column AR
  
  inventorySheet.getRange(1, 1, 1, allHeaders.length).setValues([allHeaders]);
  inventorySheet.getRange(1, 1, 1, allHeaders.length)
    .setFontWeight('bold')
    .setBackground('#ff9800')
    .setFontColor('white')
    .setVerticalAlignment('middle')
    .setHorizontalAlignment('center')
    .setBorder(true, true, true, true, true, true, '#e65100', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
  
  // Add instruction row
  const instructionRow = ['Paste your CIN7 inventory export data below this row →'];
  for (let i = 1; i < allHeaders.length; i++) {
    instructionRow.push('');
  }
  inventorySheet.getRange(2, 1, 1, allHeaders.length).setValues([instructionRow]);
  inventorySheet.getRange(2, 1, 1, allHeaders.length)
    .setFontStyle('italic')
    .setFontColor('#666666')
    .setBackground('#fff8e1')
    .setVerticalAlignment('middle')
    .setBorder(true, true, true, true, true, true, '#ffecb3', SpreadsheetApp.BorderStyle.SOLID);
  
  // Set column widths for visible columns
  inventorySheet.setColumnWidth(1, 120); // ProductCode
  inventorySheet.setColumnWidth(2, 250); // Name
  inventorySheet.setColumnWidth(3, 100); // Category
  inventorySheet.setColumnWidth(4, 120); // Brand
  inventorySheet.setColumnWidth(5, 100); // Type
  inventorySheet.setColumnWidth(6, 200); // Description
  inventorySheet.setColumnWidth(7, 100); // StockLevel
  inventorySheet.setColumnWidth(8, 100); // UnitCost
  
  // Highlight important columns
  inventorySheet.getRange(1, 44).setBackground('#ffeb3b'); // Unit column (AR)
  
  // Set row heights
  inventorySheet.setRowHeight(1, 35);
  inventorySheet.setRowHeight(2, 30);
}

/**
 * Validate imported data and show summary
 */
function validateImportedData() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const bomsSheet = ss.getSheetByName('BOMs');
  const inventorySheet = ss.getSheetByName('Inventory');
  
  let report = '📊 Data Import Validation Report\n\n';
  
  // Check BOMs data
  if (bomsSheet && bomsSheet.getLastRow() > 2) {
    const bomRowCount = bomsSheet.getLastRow() - 2; // Exclude header and instruction rows
    report += `✅ BOMs Sheet: ${bomRowCount} rows imported\n`;
    
    // Check for required columns
    const bomData = bomsSheet.getRange(3, 1, Math.min(5, bomRowCount), 7).getValues();
    const hasProductSKU = bomData.some(row => row[1] && row[1].toString().trim() !== '');
    const hasComponentSKU = bomData.some(row => row[3] && row[3].toString().trim() !== '');
    
    if (hasProductSKU && hasComponentSKU) {
      report += '   • Product and Component SKUs detected ✅\n';
    } else {
      report += '   • ⚠️ Missing SKU data - check column mapping\n';
    }
  } else {
    report += '❌ BOMs Sheet: No data found\n';
  }
  
  // Check Inventory data
  if (inventorySheet && inventorySheet.getLastRow() > 2) {
    const inventoryRowCount = inventorySheet.getLastRow() - 2;
    report += `✅ Inventory Sheet: ${inventoryRowCount} rows imported\n`;
    
    // Check for slush products
    const inventoryData = inventorySheet.getRange(3, 1, Math.min(50, inventoryRowCount), 5).getValues();
    const slushProducts = inventoryData.filter(row => {
      const name = row[1] ? row[1].toString().toLowerCase() : '';
      const category = row[2] ? row[2].toString().toLowerCase() : '';
      const brand = row[3] ? row[3].toString().toLowerCase() : '';
      const type = row[4] ? row[4].toString().toLowerCase() : '';
      
      return name.includes('slush') || category.includes('slush') || 
             brand.includes('slush') || type.includes('slush');
    });
    
    report += `   • ${slushProducts.length} slush products found ✅\n`;
    
    if (slushProducts.length > 0) {
      report += '   • Ready for dropdown setup ✅\n';
    }
  } else {
    report += '❌ Inventory Sheet: No data found\n';
  }
  
  report += '\n💡 Next Steps:\n';
  report += '1. If data looks good, run "Setup Finished Good Dropdown"\n';
  report += '2. Go to Dashboard and start your workflow\n';
  report += '3. If issues found, check your CSV data and re-paste\n';
  
  SpreadsheetApp.getUi().alert('Data Validation Complete', report, SpreadsheetApp.getUi().ButtonSet.OK);
}

/**
 * Clear import sheets for fresh data
 */
function clearImportSheets() {
  const ui = SpreadsheetApp.getUi();
  const result = ui.alert('Clear Import Data', 
    'This will clear all data from BOMs and Inventory sheets. Continue?', 
    ui.ButtonSet.YES_NO);
  
  if (result === ui.Button.YES) {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    // Clear BOMs sheet
    const bomsSheet = ss.getSheetByName('BOMs');
    if (bomsSheet && bomsSheet.getLastRow() > 2) {
      bomsSheet.getRange(3, 1, bomsSheet.getLastRow() - 2, bomsSheet.getLastColumn()).clearContent();
    }
    
    // Clear Inventory sheet  
    const inventorySheet = ss.getSheetByName('Inventory');
    if (inventorySheet && inventorySheet.getLastRow() > 2) {
      inventorySheet.getRange(3, 1, inventorySheet.getLastRow() - 2, inventorySheet.getLastColumn()).clearContent();
    }
    
    ui.alert('Import sheets cleared successfully!');
  }
}

/**
 * Create menu items for easy access
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('BOM Converter')
    .addItem('🚀 Initialize BOM Converter', 'initializeBOMConverter')
    .addSeparator()
    .addSubMenu(ui.createMenu('📁 Data Import')
      .addItem('Setup Import Templates', 'setupImportTemplates')
      .addItem('Validate Imported Data', 'validateImportedData')
      .addItem('Clear Import Sheets', 'clearImportSheets'))
    .addSeparator()
    .addItem('Setup Finished Good Dropdown', 'setupFinishedGoodDropdownInDashboard')
    .addSeparator()
    .addItem('Load Materials for Finished Good', 'loadMaterialsForFinishedGood')
    .addItem('Add Custom Material', 'addCustomMaterial')
    .addSeparator()
    .addItem('Calculate Per-Case Consumption', 'calculatePerCaseFromCurrentSheet')
    .addItem('Generate CIN7 BOM Export', 'generateBOMFromCurrentSheet')
    .addSeparator()
    .addItem('Debug: Show BOM Products', 'debugShowBOMProducts')
    .addToUi();
}