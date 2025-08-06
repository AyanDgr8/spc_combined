// reportFetcher.js
// Generic report fetcher for call-center portal tables.
// Supports the following endpoints:
//   – /portal/reports/cdrs                         (CDRs)
//   – /portal/callcenter/reports/queues-calls      (Queue Calls)
//   – /portal/callcenter/reports/queues-outbound-calls (Queue Outbound Calls)
//   – /portal/callcenter/reports/campaigns-activity    (Campaigns Activity)
//
// Like agentStatus.js this module handles:
//   • Portal authentication via tokenService.getPortalToken
//   – Automatic pagination via next_start_key when provided
//   • Exponential-backoff retry logic (up to 3 attempts)
//   • Optional CSV serialization helper
//   • A minimal CLI for ad-hoc usage

import axios from 'axios';
import fs from 'fs';
import path from 'path';
import { getPortalToken, httpsAgent } from './tokenService.js';
import { parsePhoneNumber, getCountries } from 'libphonenumber-js';
import ms from 'ms';

const MAX_RETRIES = 3;

const ENDPOINTS = {
  // Raw CDRs
  cdrs: '/api/v2/reports/cdrs',

  // Queue-specific CDR summaries
  queueCalls: '/api/v2/reports/queues_cdrs',                 // inbound queues
  queueOutboundCalls: '/api/v2/reports/queues_outbound_cdrs', // outbound queues

  // Campaign dialer lead activity
  campaignsActivity: '/api/v2/reports/campaigns/leads/history'
};

// Simple in-memory cache (per Node process). In production replace with Redis.
const CACHE_TTL = ms('5m');          // 5 minutes
const reportCache = new Map();       // Map<cacheKey,{expires:number,data:object[]}>

// Generate a unique key from report + tenant + window params.
function makeCacheKey(report, tenant, params) {
  const { startDate = '', endDate = '' } = params || {};
  return `${report}|${tenant}|${startDate}|${endDate}`;
}

/**
 * Convert an array of plain objects to a CSV string.
 * Borrowed from agentStatus.js to avoid new deps.
 */
function toCsv(records, delimiter = ',') {
  if (!records.length) return '';
  const header = Object.keys(records[0]).join(delimiter);
  const rows = records.map(r =>
    Object.values(r)
      .map(v => {
        if (v == null) return '';
        const str = String(v);
        return str.includes(delimiter) || str.includes('\n') || str.includes('"')
          ? `"${str.replace(/"/g, '""')}"` // RFC4180 escaping
          : str;
      })
      .join(delimiter)
  );
  return [header, ...rows].join('\n');
}

/**
 * Extract country name from a phone number
 * @param {string} phoneNumber - The phone number to parse
 * @returns {string} - Country name or empty string if not found
 */
function extractCountryFromPhoneNumber(phoneNumber) {
  if (!phoneNumber || typeof phoneNumber !== 'string') {
    return '';
  }

  try {
    // Clean the phone number by removing spaces, dashes, and other non-digit characters except +
    let cleanNumber = phoneNumber.replace(/[^\d+]/g, '');
    
    // Strip all leading zeros (except if it starts with +)
    if (!cleanNumber.startsWith('+')) {
      cleanNumber = cleanNumber.replace(/^0+/, '');
    }
    
    // Handle very short numbers (likely internal extensions)
    if (cleanNumber.length <= 4) {
      return ''; // Don't try to parse internal extensions
    }
    
    // Handle international UAE numbers first
    if (cleanNumber.startsWith('+971') || cleanNumber.startsWith('971')) {
      return 'UAE';
    }
    
    // Handle Egyptian numbers
    if (cleanNumber.startsWith('+20') || cleanNumber.startsWith('20')) {
      return 'Egypt';
    }
    
    // Handle Indian numbers
    if (cleanNumber.length === 10 && cleanNumber.startsWith('9')) {
      return 'India';
    }
    if (cleanNumber.length === 10 && cleanNumber.startsWith('8')) {
      return 'India';
    }
    if (cleanNumber.length === 10 && cleanNumber.startsWith('7')) {
      return 'India';
    }
    if (cleanNumber.length === 10 && cleanNumber.startsWith('6')) {
      return 'India';
    }
    
    // Handle UAE numbers (after stripping leading zeros)
    // UAE mobile numbers: 5XXXXXXXX (9 digits after stripping leading 0)
    if (cleanNumber.length === 9 && cleanNumber.startsWith('5')) {
      return 'UAE';
    }
    
    // UAE mobile numbers: 5XXXXXXX (8 digits after stripping leading 05)
    if (cleanNumber.length === 8 && cleanNumber.startsWith('5')) {
      return 'UAE';
    }
    
    // UAE mobile numbers: 5XXXXXX (7 digits after stripping leading 055)
    if (cleanNumber.length === 7 && cleanNumber.startsWith('5')) {
      return 'UAE';
    }
    
    // UAE mobile numbers: 10 digits starting with 5 (e.g., 5901181682)
    if (cleanNumber.length === 10 && cleanNumber.startsWith('5')) {
      return 'UAE';
    }
    
    // UAE landline numbers: 4XXXXXXX, 2XXXXXXX, 3XXXXXXX, 6XXXXXXX, 7XXXXXXX
    if (cleanNumber.length === 8 && (
      cleanNumber.startsWith('4') || cleanNumber.startsWith('2') || 
      cleanNumber.startsWith('3') || cleanNumber.startsWith('6') || 
      cleanNumber.startsWith('7')
    )) {
      return 'UAE';
    }
    
    // Handle UK numbers (10-11 digits starting with 1)
    if ((cleanNumber.length === 10 || cleanNumber.length === 11) && cleanNumber.startsWith('1')) {
      return 'UK';
    }
    
    // Handle UK numbers starting with 44 (country code)
    if (cleanNumber.startsWith('44') && cleanNumber.length >= 10) {
      return 'UK';
    }
    
    // Handle Egypt numbers (10 digits starting with 10)
    if (cleanNumber.length === 10 && cleanNumber.startsWith('10')) {
      return 'Egypt';
    }
    
    // If number starts with multiple zeros, strip them and add + prefix
    if (cleanNumber.startsWith('00')) {
      cleanNumber = '+' + cleanNumber.substring(2);
    } else if (cleanNumber.startsWith('0') && cleanNumber.length > 10) {
      // For numbers like 0568334181, this might be a local format, try with +
      cleanNumber = '+' + cleanNumber.substring(1);
    } else if (!cleanNumber.startsWith('+') && cleanNumber.length > 10) {
      // For numbers like 9715866875457, add + prefix
      cleanNumber = '+' + cleanNumber;
    }

    // Parse the phone number
    const phoneNumberObj = parsePhoneNumber(cleanNumber);
    
    if (phoneNumberObj && phoneNumberObj.country) {
      // Get country name from country code
      const countryCode = phoneNumberObj.country;
      
      // Map common country codes to names
      const countryNames = {
        'AE': 'UAE',
        'IN': 'India', 
        'GB': 'United Kingdom',
        'US': 'United States',
        'CA': 'Canada',
        'AU': 'Australia',
        'DE': 'Germany',
        'FR': 'France',
        'IT': 'Italy',
        'ES': 'Spain',
        'NL': 'Netherlands',
        'BE': 'Belgium',
        'CH': 'Switzerland',
        'AT': 'Austria',
        'SE': 'Sweden',
        'NO': 'Norway',
        'DK': 'Denmark',
        'FI': 'Finland',
        'PL': 'Poland',
        'CZ': 'Czech Republic',
        'HU': 'Hungary',
        'RO': 'Romania',
        'BG': 'Bulgaria',
        'HR': 'Croatia',
        'SI': 'Slovenia',
        'SK': 'Slovakia',
        'LT': 'Lithuania',
        'LV': 'Latvia',
        'EE': 'Estonia',
        'IE': 'Ireland',
        'PT': 'Portugal',
        'GR': 'Greece',
        'CY': 'Cyprus',
        'MT': 'Malta',
        'LU': 'Luxembourg',
        'BR': 'Brazil',
        'MX': 'Mexico',
        'AR': 'Argentina',
        'CL': 'Chile',
        'CO': 'Colombia',
        'PE': 'Peru',
        'VE': 'Venezuela',
        'UY': 'Uruguay',
        'PY': 'Paraguay',
        'BO': 'Bolivia',
        'EC': 'Ecuador',
        'CN': 'China',
        'JP': 'Japan',
        'KR': 'South Korea',
        'TH': 'Thailand',
        'VN': 'Vietnam',
        'MY': 'Malaysia',
        'SG': 'Singapore',
        'ID': 'Indonesia',
        'PH': 'Philippines',
        'TW': 'Taiwan',
        'HK': 'Hong Kong',
        'MO': 'Macau',
        'RU': 'Russia',
        'UA': 'Ukraine',
        'BY': 'Belarus',
        'KZ': 'Kazakhstan',
        'UZ': 'Uzbekistan',
        'KG': 'Kyrgyzstan',
        'TJ': 'Tajikistan',
        'TM': 'Turkmenistan',
        'AM': 'Armenia',
        'AZ': 'Azerbaijan',
        'GE': 'Georgia',
        'MD': 'Moldova',
        'EG': 'Egypt',
        'SA': 'Saudi Arabia',
        'TR': 'Turkey',
        'IL': 'Israel',
        'JO': 'Jordan',
        'LB': 'Lebanon',
        'SY': 'Syria',
        'IQ': 'Iraq',
        'IR': 'Iran',
        'AF': 'Afghanistan',
        'PK': 'Pakistan',
        'BD': 'Bangladesh',
        'LK': 'Sri Lanka',
        'NP': 'Nepal',
        'BT': 'Bhutan',
        'MV': 'Maldives',
        'ZA': 'South Africa',
        'NG': 'Nigeria',
        'KE': 'Kenya',
        'GH': 'Ghana',
        'ET': 'Ethiopia',
        'TZ': 'Tanzania',
        'UG': 'Uganda',
        'ZW': 'Zimbabwe',
        'ZM': 'Zambia',
        'MW': 'Malawi',
        'MZ': 'Mozambique',
        'BW': 'Botswana',
        'NA': 'Namibia',
        'SZ': 'Eswatini',
        'LS': 'Lesotho'
      };
      
      return countryNames[countryCode] || countryCode;
    }
  } catch (error) {
    // If parsing fails, return empty string (reduce console warnings)
    // Only log if it's not a known pattern we can't handle
    const cleanNum = phoneNumber.replace(/[^\d]/g, '');
    if (cleanNum.length > 4) { // Only log longer numbers, not extensions
      console.warn(`Failed to parse phone number: ${phoneNumber}`, error.message);
    }
  }
  
  return '';
}

/**
 * Generic report fetcher with pagination + retries.
 *
 * @param {string} report   – one of keys in ENDPOINTS.
 * @param {string} tenant   – domain / account id.
 * @param {object} params   – query params (startDate/endDate etc).
 * @returns {Promise<object[]>}
 */
export async function fetchReport(report, tenant, params = {}) {
  if (!ENDPOINTS[report]) throw new Error(`Unknown report type: ${report}`);

  // ---------------- Cache lookup ----------------
  const cacheKey = makeCacheKey(report, tenant, params);
  const cached = reportCache.get(cacheKey);
  if (cached && Date.now() < cached.expires) {
    // Return a shallow copy so callers can mutate safely
    return Array.isArray(cached.data) ? [...cached.data] : cached.data;
  }
  // ------------------------------------------------

  const url = `${process.env.BASE_URL}${ENDPOINTS[report]}`;
  let token;
  const out = [];
  let startKey;
  let nextStartKey = null;
  const maxRows = params.maxRows;

  retry: for (let attempt = 0, delay = 1_000; attempt < MAX_RETRIES; attempt++, delay *= 2) {
    try {
      while (true) {
        const qs = {
          ...params,
          // Request full set of columns for queue reports so duration, abandon etc. are returned
          ...(report === 'queueOutboundCalls' && {
            fields: [
              'called_time',
              'agent_name',
              'agent_ext',
              'destination',
              'answered_time',
              'hangup_time',
              'wait_duration',
              'talked_duration',
              'queue_name',
              'queue_history',
              'agent_history',
              'agent_hangup',
              'call_id',
              'bleg_call_id',
              'event_timestamp',
              'agent_first_name',
              'agent_last_name',
              'agent_extension',
              'agent_email',
              'agent_talk_time',
              'agent_connect_time',
              'agent_action',
              'agent_transfer',
              'csat',
              'media_recording_id',
              'recording_filename',
              'caller_id_name',
              'caller_id_number',
              'a_leg',
              'interaction_id',
              'agent_disposition',
              'agent_subdisposition'
            ].join(',')
          }),
          // Same for inbound queue calls (queues_cdrs) so we get talked_duration & abandoned columns
          ...(report === 'queueCalls' && {
            fields: [
              'called_time',
              'caller_id_number',
              'caller_id_name',
              'answered_time',
              'hangup_time',
              'wait_duration',
              'talked_duration',
              'queue_name',
              'abandoned',
              'queue_history',
              'agent_history',
              'agent_attempts',
              'agent_hangup',
              'call_id',
              'bleg_call_id',
              'event_timestamp',
              'agent_first_name',
              'agent_last_name',
              'agent_extension',
              'agent_email',
              'agent_talk_time',
              'agent_connect_time',
              'agent_action',
              'agent_transfer',
              'csat',
              'media_recording_id',
              'recording_filename',
              'callee_id_number',
              'a_leg',
              'interaction_id',
              'agent_disposition',
              'agent_subdisposition'
            ].join(',')
          }),
          // Request all relevant columns for campaign activity
          ...(report === 'campaignsActivity' && {
            fields: [
              'datetime',
              'timestamp',
              'campaign_name',
              'campaign_type',
              'lead_name',
              'lead_first_name',
              'lead_last_name',
              'lead_number',
              'lead_ticket_id',
              'lead_type',
              'agent_name',
              'agent_extension',
              'agent_talk_time',
              'lead_history',
              'call_id',
              'campaign_timestamps',
              'media_recording_id',
              'recording_filename',
              'status',
              'customer_wait_time_sla',
              'customer_wait_time_over_sla',
              'disposition',
              'hangup_cause',
              'lead_disposition',
              'agent_subdisposition',
              'answered_time'
            ].join(',')
          }),
          // Same for CDRS calls (cdrs) so we get talked_duration & abandoned columns
          ...(report === 'cdrs' && {
            fields: [
              'call_id',
              'datetime',
              'timestamp',
              'caller_id_name',
              'caller_id_number',
              'callee_id_name',
              'callee_id_number',
              'to',
              'from',
              'duration_seconds',
              'billing_seconds',
              'ringing_seconds',
              'hangup_cause',
              'media_recording_id',
              'recording_filename',
              'a_leg',
              'interaction_id',
              'answered_time',
            ].join(',')
          }),
          ...(startKey && { start_key: startKey })
        };

        // Acquire/refresh token for every loop iteration (cheap due to cache)
        token = await getPortalToken(tenant);

        const resp = await axios.get(url, {
          params: qs,
          headers: {
            Authorization: `Bearer ${token}`,
            'X-User-Agent': 'portal',
            'X-Account-ID': process.env.ACCOUNT_ID_HEADER ?? tenant
          },
          httpsAgent
        });

        const payload = resp.data;

        // Always capture paging token; undefined → null to signal end of list
        nextStartKey = payload.next_start_key ?? null;

        let records;
        if (Array.isArray(payload?.data)) {
          records = payload.data;
        } else if (Array.isArray(payload)) {
          // Some endpoints return an array at top-level
          records = payload;
        } else if (payload.rows && Array.isArray(payload.rows)) {
          records = payload.rows;
        } else {
          // fallback – attempt to flatten object of objects (similar to agentStatus)
          records = Object.entries(payload).map(([k, v]) => ({ key: k, ...v }));
        }

        const remaining = maxRows ? (maxRows - out.length) : records.length;

        // Push at most `remaining` records so we never exceed requested limit
        if (remaining > 0) {
          out.push(...records.slice(0, remaining));
        }

        // NEW: break early once we have *some* rows so caller can respond
        // quickly. We keep nextStartKey so the very next request can resume
        // where we left off.
        if (out.length > 0) {
          break;
        }

        // If we still didn't accumulate anything and there is another page,
        // continue looping; otherwise exit.
        if (nextStartKey === null) {
          break;
        }

        startKey = nextStartKey;
      }
      break retry; // success
    } catch (err) {
      if (attempt === MAX_RETRIES - 1) throw err;
      console.warn(`Report fetch failed (${err.message}); retrying in ${delay}ms…`);
      await new Promise(r => setTimeout(r, delay));
    }
  }

  // ---------------------------------------------------------------------------
  // Post-processing helpers
  // Add Extension column logic for all report types
  out.forEach(record => {
    let extension = '';
    
    if (report === 'campaignsActivity') {
      // For Campaign: use agent_extension from main JSON response
      extension = record.agent_extension || '';
    } else if (report === 'queueCalls' || report === 'queueOutboundCalls') {
      // For Inbound/Outbound: get ext from first agent_history record
      if (Array.isArray(record.agent_history) && record.agent_history.length > 0) {
        const firstAgentHistory = record.agent_history[0];
        extension = firstAgentHistory.ext || '';
      }
    } else if (report === 'cdrs') {
      // For CDRs: leave blank
      extension = '';
    }
    
    // Add Extension column to the record
    record.Extension = extension;
  });

  // Add Country column logic for all report types
  out.forEach(record => {
    let country = '';
    
    if (report === 'campaignsActivity') {
      // For Campaign: use lead_number from main JSON response
      country = extractCountryFromPhoneNumber(record.lead_number) || '';
    } else if (report === 'queueCalls') {
      // For Inbound calls: get country from caller_id_number (the person calling in)
      country = extractCountryFromPhoneNumber(record.caller_id_number) || '';
    } else if (report === 'queueOutboundCalls') {
      // For Outbound calls: get country from 'to' field (the person being called)
      console.log(`DEBUG: Outbound call - to: "${record.to}"`);
      console.log(`DEBUG: Outbound call - destination: "${record.destination}"`);
      console.log(`DEBUG: Outbound call - callee_id_number: "${record.callee_id_number}"`);
      console.log(`DEBUG: Record keys:`, Object.keys(record));
      
      // Try 'to' field first, then fallback to destination or callee_id_number
      const phoneNumber = record.to || record.destination || record.callee_id_number;
      console.log(`DEBUG: Using phone number: "${phoneNumber}"`);
      
      country = extractCountryFromPhoneNumber(phoneNumber) || '';
      console.log(`DEBUG: Extracted country: "${country}"`);
    } else if (report === 'cdrs') {
      // For CDRs: get country from caller_id_number or callee_id_number
      country = extractCountryFromPhoneNumber(record.caller_id_number) || extractCountryFromPhoneNumber(record.callee_id_number) || '';
    }
    
    // Add Country column to the record
    record.Country = country;
  });

  if (report === 'queueCalls' || report === 'queueOutboundCalls') {
    // Derive durations if the backend omitted them (older Talkdesk tenants)
    out.forEach(record => {
      // Talked duration
      if (!record.talked_duration && record.hangup_time && record.answered_time) {
        record.talked_duration = record.hangup_time - record.answered_time;
      }
      // Wait / queue duration
      if (!record.wait_duration && record.called_time) {
        if (record.answered_time) {
          record.wait_duration = record.answered_time - record.called_time;
        } else if (record.hangup_time) {
          record.wait_duration = record.hangup_time - record.called_time;
        }
      }
    });
  }

  // For inbound queue reports Talkdesk returns one row per agent leg.
  // When the consumer only needs a single row per call we keep the *first*
  // occurrence for each call_id (usually the initial `dial` leg) and drop the rest.
  if (report === 'queueCalls') {
    const seen = new Set();
    const firstRows = [];
    for (const rec of out) {
      // If the row is missing a call_id we cannot group it – keep it.
      if (!rec.call_id) {
        firstRows.push(rec);
        continue;
      }
      if (!seen.has(rec.call_id)) {
        seen.add(rec.call_id);
        firstRows.push(rec);
      }
    }

    // Derive `abandoned` flag when Talkdesk omits it
    // Business rule: if agent_history missing/empty, OR
    //                answered_time is falsy (not set), OR
    //                all agent_history entries lack answered_time
    firstRows.forEach(r => {
      const hist = r.agent_history;
      const histMissing =
        hist == null ||
        (Array.isArray(hist) && hist.length === 0) ||
        // Handle cases where API returns array with empty objects [{}]
        (Array.isArray(hist) && hist.every(h => h && Object.keys(h).length === 0));

      let histNoAnswer = false;
      if (Array.isArray(hist) && hist.length > 0) {
        histNoAnswer = hist.every(h => !h?.answered_time && !h?.agent_action?.includes("transfer"));
      }

      const isAbandoned = histMissing || !r.answered_time || histNoAnswer;

      // Always override to ensure consistency
      r.abandoned = isAbandoned ? "YES" : "NO";
    });

    // Cache result BEFORE returning
    reportCache.set(cacheKey, { expires: Date.now() + CACHE_TTL, data: firstRows });
    return { rows: firstRows, next: nextStartKey };
  }

  // For outbound queue reports the API returns one row but embeds full
  // queue history as an array.  Keep only the first queue_history element
  // (oldest) while leaving the full agent_history intact.
  if (report === 'queueOutboundCalls') {
    out.forEach(rec => {
      if (Array.isArray(rec.queue_history) && rec.queue_history.length > 1) {
        rec.queue_history = [rec.queue_history[0]];
      }
    });
    // Cache result BEFORE returning
    reportCache.set(cacheKey, { expires: Date.now() + CACHE_TTL, data: out });
    return { rows: out, next: nextStartKey };
  }

  // Cache result BEFORE returning
  reportCache.set(cacheKey, { expires: Date.now() + CACHE_TTL, data: out });
  return { rows: out, next: nextStartKey };
}

// Convenience wrappers
export const fetchCdrs = (tenant, opts) => fetchReport('cdrs', tenant, opts);
export const fetchQueueCalls = (tenant, opts) => fetchReport('queueCalls', tenant, opts);
export const fetchQueueOutboundCalls = (tenant, opts) => fetchReport('queueOutboundCalls', tenant, opts);
export const fetchCampaignsActivity = (tenant, opts) => fetchReport('campaignsActivity', tenant, opts);

/**
 * Minimal CLI: node -r dotenv/config reportFetcher.js <report> <tenant> <startISO> <endISO> [outfile]
 */
async function cli() {
  const [,, report, tenant, startIso, endIso, outFile] = process.argv;
  if (!report || !tenant) {
    console.error('Usage: node -r dotenv/config reportFetcher.js <report> <tenant> [startISO] [endISO] [outfile.{csv|json}]');
    console.error(`report = ${Object.keys(ENDPOINTS).join(' | ')}`);
    process.exit(1);
  }
  const params = {};
  if (startIso) {
    const startDate = Date.parse(startIso);
    if (Number.isNaN(startDate)) throw new Error('Invalid start date');
    params.startDate = Math.floor(startDate / 1000);
  }
  if (endIso) {
    const endDate = Date.parse(endIso);
    if (Number.isNaN(endDate)) throw new Error('Invalid end date');
    params.endDate = Math.floor(endDate / 1000);
  }

  const data = await fetchReport(report, tenant, params);
  console.log(`Fetched ${data.rows.length} rows for ${report}`);

  if (outFile) {
    await fs.promises.mkdir(path.dirname(outFile), { recursive: true });
    if (outFile.endsWith('.csv')) {
      await fs.promises.writeFile(outFile, toCsv(data.rows));
    } else {
      await fs.promises.writeFile(outFile, JSON.stringify(data.rows, null, 2));
    }
    console.log(`Saved to ${outFile}`);
  } else {
    console.table(data.rows);
  }
}

if (import.meta.url === process.argv[1] || import.meta.url === `file://${process.argv[1]}`) {
  cli().catch(err => {
    console.error(err.response?.data || err.stack || err.message);
    process.exit(1);
  });
}
