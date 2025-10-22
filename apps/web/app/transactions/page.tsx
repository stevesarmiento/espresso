'use client';

import { useState } from 'react';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Search } from 'lucide-react';

export default function TransactionsPage() {
  const [signature, setSignature] = useState('');

  function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    // TODO: Implement transaction search
    console.log('Searching for:', signature);
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-8">
        <h1 className="text-4xl font-bold mb-2">Transaction Search</h1>
        <p className="text-muted-foreground">
          Search for transactions by signature or program ID
        </p>
      </div>

      <Card className="max-w-2xl mx-auto">
        <CardHeader>
          <CardTitle>Search Transactions</CardTitle>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSearch} className="space-y-4">
            <div className="flex gap-2">
              <Input
                placeholder="Enter transaction signature..."
                value={signature}
                onChange={(e) => setSignature(e.target.value)}
                className="flex-1"
              />
              <Button type="submit">
                <Search className="mr-2 h-4 w-4" />
                Search
              </Button>
            </div>
          </form>

          <div className="mt-8 p-6 border rounded-lg bg-muted/50 text-center">
            <p className="text-sm text-muted-foreground">
              Transaction search functionality coming soon
            </p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

