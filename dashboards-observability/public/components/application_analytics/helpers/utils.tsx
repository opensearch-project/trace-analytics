/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// Name validation 
export const isNameValid = (name: string) => {
  let toast: string[]  = [];
  if (name.length >= 50) {
    toast.push('Name must be less than 50 characters');
  }
  if (name.length === 0) {
    toast.push('Name must be at least 1 character');
  }
  if (name.trim().length === 0) {
    toast.push('Name must have characters')
  }
  return toast;
};
