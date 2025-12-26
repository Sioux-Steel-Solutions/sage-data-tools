import React from 'react';
import { Box, Text } from 'ink';
import { TableEntry } from '../types.js';

interface FailurePromptProps {
  table: TableEntry;
}

export const FailurePrompt: React.FC<FailurePromptProps> = ({ table }) => {
  const error = table.discoveryError || table.extractionError || table.validationError || 'Unknown error';
  const phase = table.failurePhase || 'unknown';

  return (
    <Box flexDirection="column" marginTop={1} borderStyle="double" borderColor="red" paddingX={2} paddingY={1}>
      <Box>
        <Text bold color="red">
          Table Failed: {table.name}
        </Text>
      </Box>

      <Box marginTop={1} flexDirection="column">
        <Box>
          <Text dimColor>Phase: </Text>
          <Text>{phase}</Text>
        </Box>

        <Box>
          <Text dimColor>Error: </Text>
          <Text color="red">{error}</Text>
        </Box>

        {table.rowsExtracted !== undefined && table.rowsExtracted > 0 && (
          <Box marginTop={1} flexDirection="column">
            <Text bold color="yellow">Partial Extraction Data:</Text>
            <Box>
              <Text dimColor>Rows extracted: </Text>
              <Text>{table.rowsExtracted.toLocaleString()}</Text>
            </Box>
            {table.sheetsCreated && (
              <Box>
                <Text dimColor>Sheets created: </Text>
                <Text>{table.sheetsCreated}</Text>
              </Box>
            )}
          </Box>
        )}
      </Box>

      <Box marginTop={1} flexDirection="column">
        <Text bold>What would you like to do?</Text>
        <Box marginTop={1} gap={2}>
          <Text>
            <Text color="green" bold>[C]</Text>
            <Text>ontinue</Text>
          </Text>
          <Text>
            <Text color="yellow" bold>[R]</Text>
            <Text>etry</Text>
          </Text>
          <Text>
            <Text color="red" bold>[A]</Text>
            <Text>bort</Text>
          </Text>
        </Box>
      </Box>
    </Box>
  );
};
