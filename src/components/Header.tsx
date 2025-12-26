import React from 'react';
import { Box, Text } from 'ink';

export const Header: React.FC = () => {
  return (
    <Box flexDirection="column" borderStyle="round" borderColor="cyan" paddingX={2}>
      <Text bold color="cyan">
        Sage / ProvideX Data Extraction Tool
      </Text>
      <Text dimColor>Read-only extraction with validation</Text>
    </Box>
  );
};
