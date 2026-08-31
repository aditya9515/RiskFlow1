import type { Metadata } from "next";

import "./globals.css";

export const metadata: Metadata = {
  title: "RiskFlow Control Room",
  description: "Operational payment risk and decisioning dashboard",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
