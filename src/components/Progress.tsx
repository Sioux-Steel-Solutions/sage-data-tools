import React from 'react';
import { Box, Text } from 'ink';
import { Manifest } from '../types.js';

interface ProgressProps {
  manifest: Manifest;
}

export const Progress: React.FC<ProgressProps> = ({ manifest }) => {
  const { summary } = manifest;
  const processed = summary.validated + summary.failed + summary.skipped;
  const percent = summary.total > 0 ? Math.round((processed / summary.total) * 100) : 0;

  // Create a progress bar
  const barWidth = 30;
  const filled = Math.round((processed / summary.total) * barWidth);
  const empty = barWidth - filled;
  const bar = '█'.repeat(filled) + '░'.repeat(empty);

  return (
    <Box flexDirection="column" marginTop={1}>
      <Box>
        <Text>Progress: </Text>
        <Text color="green">{bar}</Text>
        <Text> {percent}%</Text>
      </Box>

      <Box marginTop={1} gap={2}>
        <Text>
          <Text dimColor>Total:</Text> {summary.total}
        </Text>
        <Text>
          <Text color="green">Validated:</Text> {summary.validated}
        </Text>
        <Text>
          <Text color="red">Failed:</Text> {summary.failed}
        </Text>
        <Text>
          <Text color="yellow">Skipped:</Text> {summary.skipped}
        </Text>
      </Box>
    </Box>
  );
};
