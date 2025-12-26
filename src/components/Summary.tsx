import React from 'react';
import { Box, Text } from 'ink';
import { Manifest } from '../types.js';

interface SummaryProps {
  manifest: Manifest;
}

export const Summary: React.FC<SummaryProps> = ({ manifest }) => {
  const { summary, tables } = manifest;

  const failedTables = tables.filter((t) => t.status === 'failed');
  const validatedTables = tables.filter((t) => t.status === 'validated');

  const totalRows = validatedTables.reduce((acc, t) => acc + (t.rowsExtracted || 0), 0);

  return (
    <Box flexDirection="column" marginTop={1}>
      <Box borderStyle="double" borderColor="green" paddingX={2} paddingY={1}>
        <Text bold color="green">
          Extraction Complete!
        </Text>
      </Box>

      <Box marginTop={1} flexDirection="column">
        <Text bold>Summary:</Text>
        <Box marginLeft={2} flexDirection="column">
          <Text>
            <Text dimColor>Total tables processed:</Text> {summary.total}
          </Text>
          <Text>
            <Text color="green">Successfully validated:</Text> {summary.validated}
          </Text>
          <Text>
            <Text color="red">Failed:</Text> {summary.failed}
          </Text>
          <Text>
            <Text color="yellow">Skipped:</Text> {summary.skipped}
          </Text>
          <Text>
            <Text dimColor>Total rows extracted:</Text> {totalRows.toLocaleString()}
          </Text>
        </Box>
      </Box>

      {failedTables.length > 0 && (
        <Box marginTop={1} flexDirection="column">
          <Text bold color="red">
            Failed Tables:
          </Text>
          <Box marginLeft={2} flexDirection="column">
            {failedTables.slice(0, 10).map((t) => (
              <Text key={t.name}>
                <Text color="red">- {t.name}:</Text>{' '}
                <Text dimColor>
                  {t.discoveryError || t.extractionError || t.validationError}
                </Text>
              </Text>
            ))}
            {failedTables.length > 10 && (
              <Text dimColor>... and {failedTables.length - 10} more</Text>
            )}
          </Box>
        </Box>
      )}

      <Box marginTop={1}>
        <Text dimColor>
          Output: {manifest.tables.length > 0 ? './exports/' : 'No tables exported'}
        </Text>
      </Box>
      <Box>
        <Text dimColor>Manifest: ./manifest.json</Text>
      </Box>
    </Box>
  );
};
