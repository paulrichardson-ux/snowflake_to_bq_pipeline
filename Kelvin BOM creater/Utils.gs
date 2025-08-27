/**
 * Utility functions for CIN7 Core BOM Converter
 * 
 * This file contains helper functions, data validation, and advanced features
 * for the BOM conversion process.
 */

/**
 * Data validation utilities
 */
const DataValidator = {
  
  /**
   * Validate that required sheets exist
   */
  validateRequiredSheets: function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const requiredSheets = [SHEET_NAMES.BOMS, SHEET_NAMES.INVENTORY];
    const missingSheets = [];
    
    requiredSheets.forEach(sheetName => {
      try {
        ss.getSheetByName(sheetName);
      } catch (e) {
        missingSheets.push(sheetName);
      }
    });
    
    if (missingSheets.length > 0) {
      throw new Error(`Missing required sheets: ${missingSheets.join(', ')}`);
    }
    
    return true;
  },
  
  /**
   * Validate BOM data structure
   */
  validateBOMData: function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const bomSheet = ss.getSheetByName(SHEET_NAMES.BOMS);
    
    if (bomSheet.getLastRow() < 2) {
      throw new Error('BOM sheet appears to be empty or has no data rows');
    }
    
    const headerRow = bomSheet.getRange(1, 1, 1, bomSheet.getLastColumn()).getValues()[0];
    const requiredColumns = ['finished_good', 'raw_material', 'quantity'];
    
    // Check if we can find reasonable column headers (case-insensitive)
    const normalizedHeaders = headerRow.map(h => h.toString().toLowerCase().replace(/[^a-z]/g, '_'));
    const missingColumns = requiredColumns.filter(col => 
      !normalizedHeaders.some(header => header.includes(col.replace('_', '')))
    );
    
    if (missingColumns.length > 0) {
      Logger.log('Warning: Could not identify all required BOM columns. Please verify column mapping.');
    }
    
    return true;
  },
  
  /**
   * Validate inventory data structure
   */
  validateInventoryData: function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const inventorySheet = ss.getSheetByName(SHEET_NAMES.INVENTORY);
    
    if (inventorySheet.getLastRow() < 2) {
      throw new Error('Inventory sheet appears to be empty or has no data rows');
    }
    
    return true;
  },
  
  /**
   * Validate numeric input
   */
  validateNumericInput: function(value, fieldName, allowZero = false) {
    const num = parseFloat(value);
    
    if (isNaN(num)) {
      throw new Error(`${fieldName} must be a valid number`);
    }
    
    if (!allowZero && num <= 0) {
      throw new Error(`${fieldName} must be greater than zero`);
    }
    
    return num;
  }
};

/**
 * Data processing utilities
 */
const DataProcessor = {
  
  /**
   * Get inventory lookup table
   */
  getInventoryLookup: function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const inventorySheet = ss.getSheetByName(SHEET_NAMES.INVENTORY);
    const inventoryData = inventorySheet.getDataRange().getValues();
    
    const lookup = {
      byCode: {},
      byName: {},
      stockItems: []
    };
    
    // Skip header row
    for (let i = 1; i < inventoryData.length; i++) {
      const row = inventoryData[i];
      const code = row[COLUMNS.INVENTORY.CODE];
      const name = row[COLUMNS.INVENTORY.NAME];
      const category = row[COLUMNS.INVENTORY.CATEGORY];
      
      if (code) {
        lookup.byCode[code] = {
          name: name,
          category: category,
          code: code
        };
      }
      
      if (name) {
        lookup.byName[name] = {
          name: name,
          category: category,
          code: code
        };
      }
      
      // Track stock items (finished goods)
      if (category && category.toString().toLowerCase() === 'stock') {
        lookup.stockItems.push({
          code: code,
          name: name
        });
      }
    }
    
    return lookup;
  },
  
  /**
   * Get BOM data for a specific finished good
   */
  getBOMData: function(finishedGood) {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const bomSheet = ss.getSheetByName(SHEET_NAMES.BOMS);
    const bomData = bomSheet.getDataRange().getValues();
    
    const materials = [];
    
    // Skip header row and find matching BOMs
    for (let i = 1; i < bomData.length; i++) {
      const row = bomData[i];
      
      // Match by name or code
      if (row[COLUMNS.BOM.FINISHED_GOOD] === finishedGood) {
        materials.push({
          rawMaterial: row[COLUMNS.BOM.RAW_MATERIAL],
          quantity: parseFloat(row[COLUMNS.BOM.QUANTITY]) || 0
        });
      }
    }
    
    return materials;
  },
  
  /**
   * Clean and normalize material data
   */
  normalizeMaterialData: function(materials, inventoryLookup) {
    return materials.map(material => {
      const code = material.rawMaterial;
      const inventoryItem = inventoryLookup.byCode[code];
      
      return {
        code: code,
        name: inventoryItem ? inventoryItem.name : code,
        bomQuantity: material.quantity,
        totalConsumed: 0,
        perCaseCalculated: 0
      };
    });
  }
};

/**
 * Export utilities
 */
const ExportUtils = {
  
  /**
   * Format data for CIN7 Core BOM import
   */
  formatForCIN7Export: function(finishedGood, materials, inventoryLookup) {
    const finishedGoodData = inventoryLookup.byName[finishedGood] || { code: finishedGood };
    
    return materials
      .filter(material => material.perCaseCalculated > 0)
      .map(material => ({
        parentItemCode: finishedGoodData.code,
        parentItemName: finishedGood,
        componentItemCode: material.code,
        componentItemName: material.name,
        quantity: material.perCaseCalculated,
        unit: 'EA' // Default unit, can be customized
      }));
  },
  
  /**
   * Validate export data before generation
   */
  validateExportData: function(exportData) {
    if (!exportData || exportData.length === 0) {
      throw new Error('No valid export data generated. Please check material consumption values.');
    }
    
    // Check for missing required fields
    const invalidRows = exportData.filter(row => 
      !row.parentItemCode || !row.componentItemCode || !row.quantity
    );
    
    if (invalidRows.length > 0) {
      throw new Error(`${invalidRows.length} rows have missing required data`);
    }
    
    return true;
  }
};

/**
 * UI utilities
 */
const UIUtils = {
  
  /**
   * Show progress dialog
   */
  showProgress: function(message) {
    const ui = SpreadsheetApp.getUi();
    // Note: Google Apps Script doesn't support true progress bars
    // This is a simple notification approach
    Logger.log(`Progress: ${message}`);
  },
  
  /**
   * Show confirmation dialog
   */
  showConfirmation: function(title, message) {
    const ui = SpreadsheetApp.getUi();
    const result = ui.alert(title, message, ui.ButtonSet.YES_NO);
    return result === ui.Button.YES;
  },
  
  /**
   * Show error dialog with detailed information
   */
  showError: function(error, context = '') {
    const ui = SpreadsheetApp.getUi();
    const errorMessage = context ? `${context}\n\nError: ${error.message}` : error.message;
    ui.alert('Error', errorMessage, ui.ButtonSet.OK);
    
    // Log detailed error for debugging
    Logger.log(`Error in ${context}: ${error.toString()}`);
  },
  
  /**
   * Format sheet for better readability
   */
  formatSheet: function(sheet, headerRow = 1) {
    // Format header row
    const headerRange = sheet.getRange(headerRow, 1, 1, sheet.getLastColumn());
    headerRange.setFontWeight('bold')
              .setBackground('#4285f4')
              .setFontColor('white')
              .setBorder(true, true, true, true, true, true);
    
    // Auto-resize columns
    sheet.autoResizeColumns(1, sheet.getLastColumn());
    
    // Freeze header row
    sheet.setFrozenRows(headerRow);
  }
};

/**
 * Backup and restore utilities
 */
const BackupUtils = {
  
  /**
   * Create backup of current materials data
   */
  backupMaterialsData: function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const materialsSheet = ss.getSheetByName(SHEET_NAMES.MATERIALS);
    
    if (materialsSheet.getLastRow() < 2) {
      return null; // No data to backup
    }
    
    const timestamp = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyyMMdd_HHmmss');
    const backupSheetName = `Materials_Backup_${timestamp}`;
    
    const backupSheet = materialsSheet.copyTo(ss);
    backupSheet.setName(backupSheetName);
    
    return backupSheetName;
  },
  
  /**
   * Clean up old backup sheets (keep last 5)
   */
  cleanupBackups: function() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheets = ss.getSheets();
    
    const backupSheets = sheets
      .filter(sheet => sheet.getName().startsWith('Materials_Backup_'))
      .sort((a, b) => a.getName().localeCompare(b.getName()))
      .reverse(); // Most recent first
    
    // Keep only the 5 most recent backups
    if (backupSheets.length > 5) {
      const sheetsToDelete = backupSheets.slice(5);
      sheetsToDelete.forEach(sheet => {
        ss.deleteSheet(sheet);
      });
    }
  }
};

/**
 * Advanced calculation utilities
 */
const CalculationUtils = {
  
  /**
   * Calculate material variance from BOM
   */
  calculateVariance: function(bomQuantity, actualPerCase) {
    const variance = actualPerCase - bomQuantity;
    const percentageVariance = bomQuantity > 0 ? (variance / bomQuantity) * 100 : 0;
    
    return {
      absolute: variance,
      percentage: percentageVariance
    };
  },
  
  /**
   * Calculate total batch cost (if cost data available)
   */
  calculateBatchCost: function(materials, costLookup = {}) {
    let totalCost = 0;
    
    materials.forEach(material => {
      const unitCost = costLookup[material.code] || 0;
      const materialCost = material.totalConsumed * unitCost;
      totalCost += materialCost;
    });
    
    return totalCost;
  },
  
  /**
   * Round to appropriate decimal places based on quantity
   */
  smartRound: function(value) {
    if (value >= 100) return Math.round(value * 100) / 100; // 2 decimal places
    if (value >= 1) return Math.round(value * 1000) / 1000; // 3 decimal places
    return Math.round(value * 10000) / 10000; // 4 decimal places
  }
};

/**
 * Error handling wrapper for main functions
 */
function safeExecute(functionName, func) {
  try {
    DataValidator.validateRequiredSheets();
    return func();
  } catch (error) {
    UIUtils.showError(error, functionName);
    throw error;
  }
}

/**
 * Enhanced versions of main functions with error handling
 */
function safeLoadMaterialsForFinishedGood() {
  safeExecute('Load Materials', loadMaterialsForFinishedGood);
}

function safeCalculatePerCaseConsumption() {
  safeExecute('Calculate Per-Case Consumption', calculatePerCaseConsumption);
}

function safeGenerateBOMImportTemplate() {
  safeExecute('Generate BOM Import Template', generateBOMImportTemplate);
}
