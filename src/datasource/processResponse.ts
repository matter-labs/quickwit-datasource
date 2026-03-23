import { DataFrame, DataLink, DataQueryRequest, DataQueryResponse, FieldType } from "@grafana/data";
import { getDataSourceSrv } from "@grafana/runtime";
import { BaseQuickwitDataSource } from './base';
import { DataLinkConfig, ElasticsearchQuery } from "../types";

export function getQueryResponseProcessor(datasource: BaseQuickwitDataSource, request: DataQueryRequest<ElasticsearchQuery>) {
  return {
    processResponse: (response: DataQueryResponse) => {
      response.data.forEach((dataFrame) => {
        const metrics = request.targets[0].metrics;
        if (metrics && metrics[0].type === 'logs') {
          processLogsDataFrame(datasource, dataFrame);
        }
      });
      return response;
    }
  };
}

export function processLogsDataFrame(datasource: BaseQuickwitDataSource, dataFrame: DataFrame) {
  // Ignore log volume dataframe, no need to add links or a displayed message field.
  if (!dataFrame.refId || dataFrame.refId.startsWith('log-volume')) {
    return;
  }
  // Skip empty dataframes
  if (dataFrame.length===0 || dataFrame.fields.length === 0) {
    return;
  }

  if (datasource.logMessageField) {
    reorderMessageField(datasource.logMessageField, dataFrame);
  }

  if (!datasource.dataLinks.length) {
    return;
  }

  for (const field of dataFrame.fields) {
    const linksToApply = datasource.dataLinks.filter((dataLink) => dataLink.field === field.name);

    if (linksToApply.length === 0) {
      continue;
    }

    field.config = field.config || {};
    field.config.links = [...(field.config.links || []), ...linksToApply.map(generateDataLink)];
  }
}

/**
 * Ensures the configured log message field is the first string field in the dataframe.
 * Grafana's parseLegacyLogsFrame picks the body via getFirstFieldOfType(FieldType.string),
 * so field ordering is all that's needed — no synthetic fields.
 *
 * For multiple comma-separated fields, the first field's values are replaced with
 * concatenated content from all configured fields, and the extra fields are removed.
 */
function reorderMessageField(logMessageField: string, dataFrame: DataFrame) {
  const messageFieldNames = logMessageField.split(',').map(f => f.trim()).filter(Boolean);
  const fieldIndices: number[] = [];
  for (const name of messageFieldNames) {
    const idx = dataFrame.fields.findIndex((field) => field.name === name);
    if (idx !== -1) {
      fieldIndices.push(idx);
    }
  }

  if (fieldIndices.length === 0) {
    return;
  }

  if (fieldIndices.length === 1) {
    // Single field: just reorder so it's the first string field (right after timestamp).
    const idx = fieldIndices[0];
    const field = dataFrame.fields[idx];
    if (field.type !== FieldType.string) {
      return;
    }
    // Find first non-time field position
    let insertAt = 0;
    for (let i = 0; i < dataFrame.fields.length; i++) {
      if (dataFrame.fields[i].type === FieldType.time) {
        insertAt = i + 1;
        break;
      }
    }
    if (idx === insertAt) {
      return; // Already in position
    }
    // Move field to insertAt position
    const fields = [...dataFrame.fields];
    fields.splice(idx, 1);
    fields.splice(insertAt, 0, field);
    dataFrame.fields = fields;
  } else {
    // Multiple fields: merge values into the first field, remove the rest.
    const primaryIdx = fieldIndices[0];
    const primaryField = dataFrame.fields[primaryIdx];
    const mergedValues = Array(dataFrame.length);
    for (let i = 0; i < dataFrame.length; i++) {
      const parts: string[] = [];
      for (const idx of fieldIndices) {
        const f = dataFrame.fields[idx];
        parts.push(`${f.name}=${f.values[i]}`);
      }
      mergedValues[i] = parts.join(' ');
    }
    primaryField.values = mergedValues;
    primaryField.type = FieldType.string;

    // Remove the extra fields (all except the primary)
    const extraIndices = new Set(fieldIndices.slice(1));
    const fields = dataFrame.fields.filter((_, i) => !extraIndices.has(i));

    // Reorder so the primary field is right after timestamp
    const newPrimaryIdx = fields.indexOf(primaryField);
    let insertAt = 0;
    for (let i = 0; i < fields.length; i++) {
      if (fields[i].type === FieldType.time) {
        insertAt = i + 1;
        break;
      }
    }
    if (newPrimaryIdx !== insertAt) {
      fields.splice(newPrimaryIdx, 1);
      fields.splice(insertAt, 0, primaryField);
    }
    dataFrame.fields = fields;
  }
}

function generateDataLink(linkConfig: DataLinkConfig): DataLink {
  const dataSourceSrv = getDataSourceSrv();

  if (linkConfig.datasourceUid) {
    const dsSettings = dataSourceSrv.getInstanceSettings(linkConfig.datasourceUid);

    return {
      title: linkConfig.urlDisplayLabel || '',
      url: '',
      internal: {
        query: { query: linkConfig.url },
        datasourceUid: linkConfig.datasourceUid,
        datasourceName: dsSettings?.name ?? 'Data source not found',
      },
    };
  } else {
    return {
      title: linkConfig.urlDisplayLabel || '',
      url: linkConfig.url,
    };
  }
}
