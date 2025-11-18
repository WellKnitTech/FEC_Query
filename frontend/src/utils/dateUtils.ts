/**
 * Centralized date utility functions for consistent date parsing and formatting across the frontend.
 * Handles timezone issues and provides safe date parsing and formatting.
 */

/**
 * Safely parse a date string, handling timezone issues for date-only strings (YYYY-MM-DD).
 * For date-only strings, appends 'T00:00:00' to avoid timezone shifts.
 * 
 * @param dateStr - Date string to parse (YYYY-MM-DD or ISO 8601 format)
 * @returns Parsed Date object or null if invalid
 */
export function parseDate(dateStr: string | null | undefined): Date | null {
  if (!dateStr) {
    return null;
  }

  try {
    // Handle YYYY-MM-DD format by appending time to avoid timezone issues
    if (typeof dateStr === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
      const date = new Date(dateStr + 'T00:00:00');
      if (!isNaN(date.getTime())) {
        return date;
      }
    }
    
    // Try parsing as ISO format or other standard formats
    const date = new Date(dateStr);
    if (!isNaN(date.getTime())) {
      return date;
    }
  } catch (e) {
    console.warn('Error parsing date:', dateStr, e);
  }

  return null;
}

/**
 * Format a date-only string (YYYY-MM-DD) for display.
 * Returns formatted date in "MMM DD, YYYY" format (e.g., "Jan 15, 2024").
 * 
 * @param dateStr - Date string in YYYY-MM-DD or ISO format
 * @param fallback - Fallback text to return if date is invalid (default: 'N/A')
 * @returns Formatted date string or fallback
 */
export function formatDate(dateStr: string | null | undefined, fallback: string = 'N/A'): string {
  if (!dateStr) {
    return fallback;
  }

  const date = parseDate(dateStr);
  if (!date) {
    return fallback;
  }

  try {
    return date.toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric'
    });
  } catch (e) {
    console.warn('Error formatting date:', dateStr, e);
    return fallback;
  }
}

/**
 * Format a datetime string for display.
 * Returns formatted datetime in "MMM DD, YYYY, HH:MM AM/PM" format (e.g., "Jan 15, 2024, 3:45 PM").
 * 
 * @param dateStr - Datetime string in ISO format
 * @param fallback - Fallback text to return if date is invalid (default: 'Never')
 * @returns Formatted datetime string or fallback
 */
export function formatDateTime(dateStr: string | null | undefined, fallback: string = 'Never'): string {
  if (!dateStr) {
    return fallback;
  }

  const date = parseDate(dateStr);
  if (!date) {
    return fallback;
  }

  try {
    return date.toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    });
  } catch (e) {
    console.warn('Error formatting datetime:', dateStr, e);
    return fallback;
  }
}

/**
 * Format a date for use in date inputs (YYYY-MM-DD format).
 * 
 * @param dateStr - Date string in any format
 * @returns Date string in YYYY-MM-DD format or empty string if invalid
 */
export function formatDateForInput(dateStr: string | null | undefined): string {
  if (!dateStr) {
    return '';
  }

  const date = parseDate(dateStr);
  if (!date) {
    return '';
  }

  try {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  } catch (e) {
    console.warn('Error formatting date for input:', dateStr, e);
    return '';
  }
}

/**
 * Get the timestamp (milliseconds since epoch) from a date string.
 * Useful for sorting or comparing dates.
 * 
 * @param dateStr - Date string to convert
 * @returns Timestamp in milliseconds or 0 if invalid
 */
export function getDateTimestamp(dateStr: string | null | undefined): number {
  const date = parseDate(dateStr);
  return date ? date.getTime() : 0;
}

/**
 * Convert an FEC election cycle to a date range.
 * FEC cycles: For cycle YYYY, the cycle includes contributions from (YYYY-1)-01-01 to YYYY-12-31
 * Example: Cycle 2026 includes contributions from 2025-01-01 through 2026-12-31
 * 
 * @param cycle - Election cycle year (e.g., 2026)
 * @returns Object with minDate and maxDate in YYYY-MM-DD format
 */
export function cycleToDateRange(cycle: number): { minDate: string; maxDate: string } {
  const cycleYear = cycle;
  const minDate = `${cycleYear - 1}-01-01`;
  const maxDate = `${cycleYear}-12-31`;
  return { minDate, maxDate };
}

/**
 * Format an election cycle as a date range string for display.
 * Example: Cycle 2026 -> "2025-2026"
 * 
 * @param cycle - Election cycle year (e.g., 2026)
 * @returns Formatted cycle range string
 */
export function formatCycleRange(cycle: number): string {
  return `${cycle - 1}-${cycle}`;
}

