/*
 * Copyright (C) 2012 Jason Gedge <www.gedge.ca>
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 * 
 *     The above copyright notice and this permission notice shall be included
 *     in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
package ca.gedge.radixtree;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A radix tree. Radix trees are String -> Object mappings which allow quick
 * lookups on the strings. Radix trees also make it easy to grab the objects
 * with a common prefix.
 * 
 * @see <a href="http://en.wikipedia.org/wiki/Radix_tree">Wikipedia</a>
 * 
 * @param <V>  the type of values stored in the tree
 */
public class RadixTree<V> implements Map<String, V> {
	public static final String KEY_CANNOT_BE_NULL = "key cannot be null";
	public static final String KEYS_MUST_BE_STRING_INSTANCES = "keys must be String instances";
	
	/** The root node in this tree */
	RadixTreeNode<V> root;
	
	/**
	 * Default constructor.
	 */
	public RadixTree() {
		this.root = new RadixTreeNode<V>("");
	}
	
	/**
	 * Traverses this radix tree using the given visitor. Note that the tree
	 * will be traversed in lexicographical order.
	 * 
	 * @param visitor  the visitor
	 */
	public void visit(RadixTreeVisitor<V, ?> visitor) {
		visit(root, "", "", visitor);
	}

	/**
	 * Traverses this radix tree using the given visitor. Only values with
	 * the given prefix will be visited. Note that the tree will be traversed
	 * in lexicographical order.
	 *
	 * @param visitor  the visitor
	 * @param prefix  the prefix used to restrict visitation
	 */
	public void visit(RadixTreeVisitor<V, ?> visitor, String prefix) {
		visit(root, prefix, "", visitor);
	}

	/**
	 * Visits the given node of this tree with the given prefix and visitor. Also,
	 * recursively visits the left/right subtrees of this node.
	 *
	 * @param node  the node
	 * @param prefix  the prefix
	 * @param visitor  the visitor
	 */
	synchronized private void visit(RadixTreeNode<V> node, String prefixAllowed, String prefix, RadixTreeVisitor<V, ?> visitor) {
		if(node.hasValue() && prefix.startsWith(prefixAllowed))
			if (!visitor.visit(prefix, node.getValue()))
				return;

		for(RadixTreeNode<V> child : node) {
			final int prefixLen = prefix.length();
			final String newPrefix = prefix + child.getPrefix();
			if(prefixAllowed.length() <= prefixLen
			   || newPrefix.length() <= prefixLen
			   || newPrefix.charAt(prefixLen) == prefixAllowed.charAt(prefixLen))
			{
				visit(child, prefixAllowed, newPrefix, visitor);
			}
		}
	}

	@Override
	synchronized public void clear() {
		root.getChildren().clear();
	}

	@Override
	public boolean containsKey(final Object keyToCheck) {
		if(keyToCheck == null)
			throw new NullPointerException(KEY_CANNOT_BE_NULL);

		if(!(keyToCheck instanceof String))
			throw new ClassCastException(KEYS_MUST_BE_STRING_INSTANCES);

		RadixTreeVisitor<V, Boolean> visitor = new RadixTreeVisitor<V, Boolean>() {
			boolean found = false;

			@Override
			public boolean visit(String key, V value) {
				if(key.equals(keyToCheck)) {
					found = true;
					return false;
				}
				return true;
			}

			@Override
			public Boolean getResult() {
				return found;
			}
		};
		visit(visitor, (String)keyToCheck);
		return visitor.getResult();
	}

	@Override
	public boolean containsValue(final Object val) {
		RadixTreeVisitor<V, Boolean> visitor = new RadixTreeVisitor<V, Boolean>() {
			boolean found = false;

			@Override
			public boolean visit(String key, V value) {
				if(val == value || (value != null && value.equals(val))) {
					found = true;
					return false;
				}
				return true;
			}

			@Override
			public Boolean getResult() {
				return found;
			}
		};
		visit(visitor);
		return visitor.getResult();
	}

	@Override
	public V get(final Object keyToCheck) {
		if(keyToCheck == null)
			throw new NullPointerException(KEY_CANNOT_BE_NULL);

		if(!(keyToCheck instanceof String))
			throw new ClassCastException(KEYS_MUST_BE_STRING_INSTANCES);

		RadixTreeVisitor<V, V> visitor = new RadixTreeVisitor<V, V>() {
			V result = null;

			@Override
			public boolean visit(String key, V value) {
				if(key.equals(keyToCheck)) {
					result = value;
					return false;
				}
				return true;
			}

			@Override
			public V getResult() {
				return result;
			}
		};
		visit(visitor, (String)keyToCheck);
		return visitor.getResult();
	}

	/**
	 * Gets a list of entries whose associated keys have the given prefix.
	 *
	 * @param prefix  the prefix to look for
	 *
	 * @return the list of values
	 *
	 * @throws NullPointerException if prefix is <code>null</code>
	 */
	public List<Map.Entry<String, V>> getEntriesWithPrefix(String prefix) {
		RadixTreeVisitor<V, List<Map.Entry<String, V>>> visitor = new RadixTreeVisitor<V, List<Map.Entry<String, V>>>() {
			final List<Map.Entry<String, V>> result = new ArrayList<Map.Entry<String, V>>();

			@Override
			public boolean visit(String key, V value) {
				result.add(new AbstractMap.SimpleEntry<String, V>(key, value));
				return true;
			}

			@Override
			public List<Map.Entry<String, V>> getResult() {
				return result;
			}
		};
		visit(visitor, prefix);
		return visitor.getResult();
	}

	/**
	 * Gets a list of values whose associated keys have the given prefix.
	 *
	 * @param prefix  the prefix to look for
	 *
	 * @return the list of values
	 *
	 * @throws NullPointerException if prefix is <code>null</code>
	 */
	public List<V> getValuesWithPrefix(String prefix) {
		if(prefix == null)
			throw new NullPointerException("prefix cannot be null");

		RadixTreeVisitor<V, List<V>> visitor = new RadixTreeVisitor<V, List<V>>() {
			final List<V> result = new ArrayList<V>();

			@Override
			public boolean visit(String key, V value) {
				result.add(value);
				return true;
			}

			@Override
			public List<V> getResult() {
				return result;
			}
		};
		visit(visitor, prefix);
		return visitor.getResult();
	}

	/**
	 * Gets a list of keys with the given prefix.
	 *
	 * @param prefix  the prefix to look for
	 *
	 * @return the list of prefixes
	 *
	 * @throws NullPointerException if prefix is <code>null</code>
	 */
	public List<String> getKeysWithPrefix(String prefix) {
		if(prefix == null)
			throw new NullPointerException("prefix cannot be null");

		RadixTreeVisitor<V, List<String>> visitor = new RadixTreeVisitor<V, List<String>>() {
			final List<String> result = new ArrayList<String>();

			@Override
			public boolean visit(String key, V value) {
				result.add(key);
				return true;
			}

			@Override
			public List<String> getResult() {
				return result;
			}
		};
		visit(visitor, prefix);
		return visitor.getResult();
	}

	@Override
	public boolean isEmpty() {
		return root.getChildren().isEmpty();
	}

	@Override
	public void putAll(Map<? extends String, ? extends V> map) {
		for(Map.Entry<? extends String, ? extends V> entry : map.entrySet())
			put(entry.getKey(), entry.getValue());
	}

	@Override
	public int size() {
		RadixTreeVisitor<V, Integer> visitor = new RadixTreeVisitor<V, Integer>() {
			int count = 0;

			@Override
			public boolean visit(String key, V value) {
				++count;
				return true;
			}

			@Override
			public Integer getResult() {
				return count;
			}
		};
		visit(visitor);
		return visitor.getResult();
	}

	@Override
	public Set<Map.Entry<String, V>> entrySet() {
		// TODO documentation Of Map.entrySet() specifies that this is a view of
		//      the entries, and modifications to this collection should be
		//      reflected in the parent structure
		//
		RadixTreeVisitor<V, Set<Map.Entry<String, V>>> visitor = new RadixTreeVisitor<V, Set<Map.Entry<String, V>>>() {
			final Set<Map.Entry<String, V>> result = new HashSet<Map.Entry<String, V>>();

			@Override
			public boolean visit(String key, V value) {
				result.add(new AbstractMap.SimpleEntry<String, V>(key, value));
				return true;
			}

			@Override
			public Set<Map.Entry<String, V>> getResult() {
				return result;
			}
		};
		visit(visitor);
		return visitor.getResult();
	}

	@Override
	public Set<String> keySet() {
		// TODO documentation Of Map.keySet() specifies that this is a view of
		//      the keys, and modifications to this collection should be
		//      reflected in the parent structure
		//
		RadixTreeVisitor<V, Set<String>> visitor = new RadixTreeVisitor<V, Set<String>>() {
			final Set<String> result = new TreeSet<String>();

			@Override
			public boolean visit(String key, V value) {
				result.add(key);
				return true;
			}

			@Override
			public Set<String> getResult() {
				return result;
			}
		};
		visit(visitor);
		return visitor.getResult();
	}

	@Override
	public Collection<V> values() {
		// TODO documentation Of Map.values() specifies that this is a view of
		//      the values, and modifications to this collection should be
		//      reflected in the parent structure
		//
		RadixTreeVisitor<V, Collection<V>> visitor = new RadixTreeVisitor<V, Collection<V>>() {
			final Collection<V> result = new ArrayList<V>();

			@Override
			public boolean visit(String key, V value) {
				result.add(value);
				return true;
			}

			@Override
			public Collection<V> getResult() {
				return result;
			}
		};
		visit(visitor);
		return visitor.getResult();
	}
	
	@Override
	public V put(String key, V value) {
		if(key == null)
			throw new NullPointerException(KEY_CANNOT_BE_NULL);
		
		return put(key, value, root);
	}
	
	/**
	 * Remove the value with the given key from the subtree rooted at the
	 * given node.
	 * 
	 * @param key  the key
	 * @param node  the node to start searching from
	 * 
	 * @return the old value associated with the given key, or <code>null</code>
	 *         if there was no mapping for <code>key</code> 
	 */
	synchronized private V put(String key, V value, RadixTreeNode<V> node) {
		V ret = null;
		
		final int largestPrefix = RadixTreeUtil.largestPrefixLength(key, node.getPrefix());
		if(largestPrefix == node.getPrefix().length() && largestPrefix == key.length()) {
			// Found a node with an exact match
			ret = node.getValue();
			node.setValue(value);
			node.setHasValue(true);
		} else if(largestPrefix == 0
				  || (largestPrefix < key.length() && largestPrefix >= node.getPrefix().length()))
		{
			// Key is bigger than the prefix located at this node, so we need to see if
			// there's a child that can possibly share a prefix, and if not, we just add
			// a new node to this node
			final String leftoverKey = key.substring(largestPrefix);
			
			boolean found = false;
			for(RadixTreeNode<V> child : node) {
				if(child.getPrefix().charAt(0) == leftoverKey.charAt(0)) {
					found = true;
					ret = put(leftoverKey, value, child);
					break;
				}
			}
			
			if(!found) {
				// No child exists with any prefix of the given key, so add a new one
				RadixTreeNode<V> n = new RadixTreeNode<V>(leftoverKey, value);
				node.getChildren().add(n);
			}
		} else if(largestPrefix < node.getPrefix().length()) {
			// Key and node.getPrefix() share a prefix, so split node
			final String leftoverPrefix = node.getPrefix().substring(largestPrefix);
			final RadixTreeNode<V> n = new RadixTreeNode<V>(leftoverPrefix, node.getValue());
			n.setHasValue(node.hasValue());
			n.getChildren().addAll(node.getChildren());
			
			node.setPrefix(node.getPrefix().substring(0, largestPrefix));
			node.getChildren().clear();
			node.getChildren().add(n);
			
			if(largestPrefix == key.length()) {
				// The largest prefix is equal to the key, so set this node's value
				ret = node.getValue();
				node.setValue(value);
				node.setHasValue(true);
			} else {
				// There's a leftover suffix on the key, so add another child 
				final String leftoverKey = key.substring(largestPrefix);
				final RadixTreeNode<V> keyNode = new RadixTreeNode<V>(leftoverKey, value);
				node.getChildren().add(keyNode);
				node.setHasValue(false);
			}
		} else {
			// node.getPrefix() is a prefix of key, so add as child
			final String leftoverKey = key.substring(largestPrefix);
			final RadixTreeNode<V> n = new RadixTreeNode<V>(leftoverKey, value);
			node.getChildren().add(n);
		}
		
		return ret;
	}

	@Override
	synchronized public V remove(Object key) {
		if(key == null)
			throw new NullPointerException(KEY_CANNOT_BE_NULL);
		
		if(!(key instanceof String))
			throw new ClassCastException(KEYS_MUST_BE_STRING_INSTANCES);

		// Special case for removing empty string (root node)
		final String sKey = (String)key;
		if(sKey.equals("")) {
			final V value = root.getValue();
			root.setHasValue(false);
			return value;
		}
		
		return remove(sKey, root);
	}
	
	/**
	 * Remove the value with the given key from the subtree rooted at the
	 * given node.
	 * 
	 * @param key  the key
	 * @param node  the node to start searching from
	 * 
	 * @return the value associated with the given key, or <code>null</code>
	 *         if there was no mapping for <code>key</code> 
	 */
	synchronized private V remove(String key, RadixTreeNode<V> node) {
		V ret = null;
		final Iterator<RadixTreeNode<V>> iter = node.getChildren().iterator();
		while(iter.hasNext()) {
			final RadixTreeNode<V> child = iter.next();
			final int largestPrefix = RadixTreeUtil.largestPrefixLength(key, child.getPrefix());
			if(largestPrefix == child.getPrefix().length() && largestPrefix == key.length()) {
				// Found our match, remove the value from this node
				if(child.getChildren().isEmpty()) {
					// Leaf node, simply remove from parent
					ret = child.getValue();
					iter.remove();
					break;
				} else if(child.hasValue()) {
					// Internal node
					ret = child.getValue();
					child.setHasValue(false);
					
					if(child.getChildren().size() == 1) {
						// The subchild's prefix can be reused, with a little modification
						final RadixTreeNode<V> subchild = child.getChildren().iterator().next();
						final String newPrefix = child.getPrefix() + subchild.getPrefix();
						
						// Merge child node with its single child
						child.setValue(subchild.getValue());
						child.setHasValue(subchild.hasValue());
						child.setPrefix(newPrefix);
						child.getChildren().clear();
					}
					
					break;
				}
			} else if(largestPrefix > 0 && largestPrefix < key.length()) {
				// Continue down subtree of child
				final String leftoverKey = key.substring(largestPrefix);
				ret = remove(leftoverKey, child);
				break;
			}
		}
		
		return ret;
	}
}
