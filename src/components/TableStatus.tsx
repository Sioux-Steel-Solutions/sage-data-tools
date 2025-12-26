import React from 'react';
import { Box, Text } from 'ink';
import { TableEntry } from '../types.js';

interface TableStatusProps {
  table: TableEntry;
  phase: 'discovery' | 'extraction' | 'validation';
}

const phaseLabels = {
  discovery: 'Phase 0: Discovery',
  extraction: 'Phase 1: Extraction',
  validation: 'Phase 2: Validation',
};

const phaseColors = {
  discovery: 'blue',
  extraction: 'magenta',
  validation: 'cyan',
} as const;

export const TableStatus: React.FC<TableStatusProps> = ({ table, phase }) => {
  return (
    <Box flexDirection="column" marginTop={1} borderStyle="single" borderColor={phaseColors[phase]} paddingX={2} paddingY={1}>
      <Box>
        <Text bold color={phaseColors[phase]}>
          {phaseLabels[phase]}
        </Text>
      </Box>

      <Box marginTop={1}>
        <Text>Table: </Text>
        <Text bold>{table.name}</Text>
        <Text dimColor> ({table.type})</Text>
      </Box>

      {phase === 'extraction' && table.rowsExtracted !== undefined && (
        <Box>
          <Text>Rows extracted: </Text>
          <Text color="green">{table.rowsExtracted.toLocaleString()}</Text>
          {table.sheetsCreated && table.sheetsCreated > 1 && (
            <Text dimColor> across {table.sheetsCreated} sheets</Text>
          )}
        </Box>
      )}

      {phase === 'discovery' && table.columnCount !== undefined && (
        <Box>
          <Text>Columns found: </Text>
          <Text color="green">{table.columnCount}</Text>
        </Box>
      )}

      {phase === 'validation' && (
        <Box>
          <Text>Validating row counts and schema...</Text>
        </Box>
      )}
    </Box>
  );
};
