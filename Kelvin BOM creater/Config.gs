/**
 * Configuration file for CIN7 Core BOM Converter
 * 
 * This file contains customizable settings and column mappings
 * to adapt to different CIN7 export formats.
 */

/**
 * Configuration object for customizing the BOM converter
 */
const CONFIG = {
  
  /**
   * Sheet names - customize these if your sheets have different names
   */
  SHEET_NAMES: {
    BOMS: 'BOMs',
    INVENTORY: 'Inventory', 
    DASHBOARD: 'Dashboard',
    MATERIALS: 'Materials',
    EXPORT: 'BOM_Import_Template',
    VARIANCE_ANALYSIS: 'Variance_Analysis'
  },
  
  /**
   * Column mappings for different CIN7 export formats
   * Adjust these based on your actual export column positions (0-based index)
   */
  COLUMN_MAPPINGS: {
    
    // Default inventory export format
    INVENTORY_DEFAULT: {
      CODE: 0,        // Item Code
      NAME: 1,        // Item Name
      CATEGORY: 2,    // Category
      UNIT_COST: 3,   // Unit Cost (optional)
      STOCK_LEVEL: 4  // Stock Level (optional)
    },
    
    // Alternative inventory format (if your export is different)
    INVENTORY_ALT: {
      CODE: 1,
      NAME: 2,
      CATEGORY: 5,
      UNIT_COST: 8,
      STOCK_LEVEL: 10
    },
    
    // Default BOM export format
    BOM_DEFAULT: {
      FINISHED_GOOD: 0,    // Parent/Finished Good
      RAW_MATERIAL: 1,     // Component/Raw Material
      QUANTITY: 2,         // Quantity per unit
      UNIT: 3             // Unit of measure (optional)
    },
    
    // Alternative BOM format
    BOM_ALT: {
      FINISHED_GOOD: 2,
      RAW_MATERIAL: 3,
      QUANTITY: 5,
      UNIT: 6
    }
  },
  
  /**
   * Data validation settings
   */
  VALIDATION: {
    MIN_BATCH_YIELD: 0.1,           // Minimum batch yield
    MAX_BATCH_YIELD: 100000,        // Maximum batch yield
    MIN_CONSUMPTION: 0,             // Minimum material consumption
    DECIMAL_PLACES: 6,              // Decimal places for calculations
    STOCK_CATEGORY_NAMES: ['stock', 'finished goods', 'finished', 'fg'] // Possible category names for finished goods
  },
  
  /**
   * Export format settings
   */
  EXPORT_FORMAT: {
    DEFAULT_UNIT: 'EA',             // Default unit of measure
    INCLUDE_ZERO_QUANTITIES: false, // Include materials with 0 consumption
    ROUND_QUANTITIES: true,         // Round quantities to decimal places
    EXPORT_HEADERS: [
      'Parent Item Code',
      'Parent Item Name', 
      'Component Item Code',
      'Component Item Name',
      'Quantity',
      'Unit'
    ]
  },
  
  /**
   * UI customization
   */
  UI: {
    THEME_COLOR: '#4285f4',         // Primary color for headers
    SUCCESS_COLOR: '#34a853',       // Success/completed color
    WARNING_COLOR: '#fbbc04',       // Warning color
    ERROR_COLOR: '#ea4335',         // Error color
    INPUT_COLOR: '#fff2cc',         // Input field background
    CALCULATED_COLOR: '#d9ead3'     // Calculated field background
  },
  
  /**
   * Feature flags
   */
  FEATURES: {
    ENABLE_VARIANCE_ANALYSIS: true,  // Show variance from original BOM
    ENABLE_COST_CALCULATION: false, // Calculate batch costs (requires cost data)
    ENABLE_BACKUP_SHEETS: true,     // Create backup sheets
    ENABLE_BATCH_PROCESSING: false, // Process multiple batches at once
    ENABLE_AUDIT_TRAIL: true        // Track changes and calculations
  }
};

/**
 * Get current column mapping based on configuration
 */
function getColumnMapping(sheetType, format = 'DEFAULT') {
  const mappingKey = `${sheetType}_${format}`;
  return CONFIG.COLUMN_MAPPINGS[mappingKey] || CONFIG.COLUMN_MAPPINGS[`${sheetType}_DEFAULT`];
}

/**
 * Detect finished goods category from inventory data
 */
function detectStockCategory(inventoryData) {
  if (!inventoryData || inventoryData.length < 2) {
    return 'stock'; // Default fallback
  }
  
  // Get all unique categories from the data
  const categories = new Set();
  for (let i = 1; i < inventoryData.length; i++) {
    const category = inventoryData[i][getColumnMapping('INVENTORY').CATEGORY];
    if (category) {
      categories.add(category.toString().toLowerCase().trim());
    }
  }
  
  // Find the best match for stock category
  for (const stockName of CONFIG.VALIDATION.STOCK_CATEGORY_NAMES) {
    if (categories.has(stockName)) {
      return stockName;
    }
  }
  
  // If no exact match, look for partial matches
  const categoryArray = Array.from(categories);
  for (const stockName of CONFIG.VALIDATION.STOCK_CATEGORY_NAMES) {
    const match = categoryArray.find(cat => cat.includes(stockName) || stockName.includes(cat));
    if (match) {
      return match;
    }
  }
  
  return 'stock'; // Default fallback
}

/**
 * Auto-detect column positions from headers
 */
function autoDetectColumns(sheetName, headerRow) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(sheetName);
  
  if (!sheet || sheet.getLastRow() < 1) {
    return null;
  }
  
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const normalizedHeaders = headers.map(h => h.toString().toLowerCase().replace(/[^a-z0-9]/g, ''));
  
  const detectedColumns = {};
  
  if (sheetName === CONFIG.SHEET_NAMES.INVENTORY) {
    // Detect inventory columns
    detectedColumns.CODE = findColumnIndex(normalizedHeaders, ['code', 'itemcode', 'sku', 'id']);
    detectedColumns.NAME = findColumnIndex(normalizedHeaders, ['name', 'itemname', 'description', 'title']);
    detectedColumns.CATEGORY = findColumnIndex(normalizedHeaders, ['category', 'type', 'class', 'group']);
    detectedColumns.UNIT_COST = findColumnIndex(normalizedHeaders, ['cost', 'unitcost', 'price', 'unitprice']);
    
  } else if (sheetName === CONFIG.SHEET_NAMES.BOMS) {
    // Detect BOM columns
    detectedColumns.FINISHED_GOOD = findColumnIndex(normalizedHeaders, ['parent', 'finishedgood', 'product', 'assembly']);
    detectedColumns.RAW_MATERIAL = findColumnIndex(normalizedHeaders, ['component', 'rawmaterial', 'material', 'part']);
    detectedColumns.QUANTITY = findColumnIndex(normalizedHeaders, ['quantity', 'qty', 'amount', 'usage']);
    detectedColumns.UNIT = findColumnIndex(normalizedHeaders, ['unit', 'uom', 'measure', 'unitofmeasure']);
  }
  
  return detectedColumns;
}

/**
 * Find column index by searching for keywords
 */
function findColumnIndex(headers, keywords) {
  for (let i = 0; i < headers.length; i++) {
    for (const keyword of keywords) {
      if (headers[i].includes(keyword)) {
        return i;
      }
    }
  }
  return -1; // Not found
}

/**
 * Validate and update configuration based on actual data
 */
function validateConfiguration() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const issues = [];
  
  // Check if required sheets exist
  for (const [key, sheetName] of Object.entries(CONFIG.SHEET_NAMES)) {
    if (['BOMS', 'INVENTORY'].includes(key)) { // Only check required sheets
      try {
        ss.getSheetByName(sheetName);
      } catch (e) {
        issues.push(`Required sheet '${sheetName}' not found`);
      }
    }
  }
  
  // Try to auto-detect columns
  const inventoryColumns = autoDetectColumns(CONFIG.SHEET_NAMES.INVENTORY);
  const bomColumns = autoDetectColumns(CONFIG.SHEET_NAMES.BOMS);
  
  if (inventoryColumns) {
    const missingInventoryColumns = Object.entries(inventoryColumns)
      .filter(([key, index]) => ['CODE', 'NAME', 'CATEGORY'].includes(key) && index === -1)
      .map(([key]) => key);
    
    if (missingInventoryColumns.length > 0) {
      issues.push(`Could not detect inventory columns: ${missingInventoryColumns.join(', ')}`);
    }
  }
  
  if (bomColumns) {
    const missingBomColumns = Object.entries(bomColumns)
      .filter(([key, index]) => ['FINISHED_GOOD', 'RAW_MATERIAL', 'QUANTITY'].includes(key) && index === -1)
      .map(([key]) => key);
    
    if (missingBomColumns.length > 0) {
      issues.push(`Could not detect BOM columns: ${missingBomColumns.join(', ')}`);
    }
  }
  
  return {
    isValid: issues.length === 0,
    issues: issues,
    detectedColumns: {
      inventory: inventoryColumns,
      bom: bomColumns
    }
  };
}

/**
 * Update configuration with detected columns
 */
function updateConfigurationWithDetected() {
  const validation = validateConfiguration();
  
  if (validation.detectedColumns.inventory) {
    // Update inventory column mapping
    Object.assign(CONFIG.COLUMN_MAPPINGS.INVENTORY_DEFAULT, validation.detectedColumns.inventory);
  }
  
  if (validation.detectedColumns.bom) {
    // Update BOM column mapping
    Object.assign(CONFIG.COLUMN_MAPPINGS.BOM_DEFAULT, validation.detectedColumns.bom);
  }
  
  return validation;
}

/**
 * Show configuration status
 */
function showConfigurationStatus() {
  const validation = validateConfiguration();
  const ui = SpreadsheetApp.getUi();
  
  if (validation.isValid) {
    ui.alert('Configuration Status', 'Configuration is valid and ready to use!', ui.ButtonSet.OK);
  } else {
    const message = 'Configuration Issues Found:\n\n' + validation.issues.join('\n') + 
                   '\n\nPlease check your data format and column mappings.';
    ui.alert('Configuration Status', message, ui.ButtonSet.OK);
  }
  
  // Log detailed information
  Logger.log('Configuration Status:', validation);
}

/**
 * Reset configuration to defaults
 */
function resetConfiguration() {
  const ui = SpreadsheetApp.getUi();
  const result = ui.alert('Reset Configuration', 
    'This will reset all configuration settings to defaults. Continue?', 
    ui.ButtonSet.YES_NO);
  
  if (result === ui.Button.YES) {
    // Reset column mappings to defaults
    CONFIG.COLUMN_MAPPINGS.INVENTORY_DEFAULT = {
      CODE: 0,
      NAME: 1,
      CATEGORY: 2,
      UNIT_COST: 3,
      STOCK_LEVEL: 4
    };
    
    CONFIG.COLUMN_MAPPINGS.BOM_DEFAULT = {
      FINISHED_GOOD: 0,
      RAW_MATERIAL: 1,
      QUANTITY: 2,
      UNIT: 3
    };
    
    ui.alert('Configuration reset to defaults.');
  }
}

/**
 * Export current configuration for backup
 */
function exportConfiguration() {
  const configJson = JSON.stringify(CONFIG, null, 2);
  Logger.log('Current Configuration:', configJson);
  
  const ui = SpreadsheetApp.getUi();
  ui.alert('Configuration Exported', 
    'Configuration has been logged to the Apps Script console. ' +
    'Go to Extensions > Apps Script > Executions to view the log.',
    ui.ButtonSet.OK);
}
